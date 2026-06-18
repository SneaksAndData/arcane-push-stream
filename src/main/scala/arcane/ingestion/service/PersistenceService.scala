package arcane.ingestion.service

import arcane.ingestion.config.DynamoDBConfig
import arcane.ingestion.api.v1.SchemaRef
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  DefaultCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import zio.*
import zio.aws.core.config.{AwsConfig, CommonAwsConfig}
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model.*
import zio.aws.dynamodb.model.primitives.*
import zio.aws.netty.NettyHttpClient
import zio.prelude.data.Optional

import java.net.URI
import java.util.UUID

trait PersistenceService:
  def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean]

final case class DynamoDBServiceLive(dynamo: DynamoDb, tableName: String) extends PersistenceService:

  def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean] =
    for
      id  <- ZIO.succeed(UUID.randomUUID().toString)
      now <- Clock.instant
      baseItem = Map(
        AttributeName("producer") -> AttributeValue(s = Optional.Present(StringAttributeValue(producer))),
        AttributeName("id")       -> AttributeValue(s = Optional.Present(StringAttributeValue(id))),
        // payload is Avro-binary bytes (or raw UTF-8 JSON for routes with no payloadSchema) —
        // stored as a DynamoDB Binary attribute so the downstream parquet writer can decode it
        // with the matching Avro schema.
        AttributeName("payload") -> AttributeValue(b =
          Optional.Present(BinaryAttributeValue(zio.Chunk.fromArray(payload)))
        ),
        AttributeName("createdAt") -> AttributeValue(n =
          Optional.Present(NumberAttributeValue(now.toEpochMilli.toString))
        ),
        // schemaSubject / schemaVersion identify the writer schema for the downstream router.
        // They are required by the CRD so always present.
        AttributeName("schemaSubject") -> AttributeValue(s =
          Optional.Present(StringAttributeValue(schemaRef.subject))
        ),
        AttributeName("schemaVersion") -> AttributeValue(n =
          Optional.Present(NumberAttributeValue(schemaRef.version.toString))
        )
      )
      // schemaFingerprint is only present for routes that have an Avro payloadSchema bound;
      // raw-JSON fallback routes omit it (nothing to fingerprint).
      item = schemaRef.fingerprint.fold(baseItem) { fp =>
        baseItem + (AttributeName("schemaFingerprint") -> AttributeValue(s =
          Optional.Present(StringAttributeValue(fp))
        ))
      }
      _ <- dynamo
        .putItem(PutItemRequest(tableName = TableArn(tableName), item = item))
        .mapError(_.toThrowable)
        .tapErrorCause(c => ZIO.logErrorCause(s"DynamoDB putItem failed for producer=$producer id=$id", c))
    yield true

object DynamoDBServiceLive:

  private val commonAwsConfigLayer: ZLayer[DynamoDBConfig, Nothing, CommonAwsConfig] =
    ZLayer.fromFunction { (cfg: DynamoDBConfig) =>
      CommonAwsConfig(
        region = Some(Region.of(cfg.region)),
        credentialsProvider =
          if cfg.endpoint.isDefined then
            // Localstack accepts any credentials; supply static dummies when overriding the endpoint.
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
          else DefaultCredentialsProvider.create(),
        endpointOverride = cfg.endpoint.map(URI.create),
        commonClientConfig = None
      )
    }

  private def tableExists(dynamo: DynamoDb, tableName: String): IO[Throwable, Boolean] =
    dynamo
      .describeTable(DescribeTableRequest(TableArn(tableName)))
      .as(true)
      .foldZIO(
        awsErr =>
          awsErr.toThrowable match
            case _: ResourceNotFoundException => ZIO.succeed(false)
            case other                        => ZIO.fail(other)
        ,
        ZIO.succeed(_)
      )

  private def createTable(dynamo: DynamoDb, tableName: String): IO[Throwable, Unit] =
    dynamo
      .createTable(
        CreateTableRequest(
          attributeDefinitions = Optional.Present(
            List(
              AttributeDefinition(KeySchemaAttributeName("producer"), ScalarAttributeType.S),
              AttributeDefinition(KeySchemaAttributeName("id"), ScalarAttributeType.S)
            )
          ),
          tableName = TableArn(tableName),
          keySchema = Optional.Present(
            List(
              KeySchemaElement(KeySchemaAttributeName("producer"), KeyType.HASH),
              KeySchemaElement(KeySchemaAttributeName("id"), KeyType.RANGE)
            )
          ),
          billingMode = Optional.Present(BillingMode.PAY_PER_REQUEST)
        )
      )
      .mapError(_.toThrowable)
      .unit *> ZIO.logInfo(s"Created DynamoDB table: $tableName")

  /** if the Table doesn't exist and 'autoCreateTable = true', create the tabe, otherwise throw IllegalStateException
    */
  private def ensureTable(dynamo: DynamoDb, cfg: DynamoDBConfig): IO[Throwable, Unit] =
    val check = tableExists(dynamo, cfg.tableName).flatMap {
      case true => ZIO.logInfo(s"DynamoDB table present: ${cfg.tableName}")
      case false if cfg.autoCreateTable =>
        ZIO.logInfo(s"DynamoDB table ${cfg.tableName} missing — creating (autoCreateTable=true)") *>
          createTable(dynamo, cfg.tableName)
      case false =>
        ZIO.fail(
          new IllegalStateException(
            s"DynamoDB table '${cfg.tableName}' does not exist and autoCreateTable=false"
          )
        )
    }

    check
      .tapError {
        case sdk: SdkClientException =>
          ZIO.logError(
            s"Cannot reach DynamoDB at ${cfg.endpoint.getOrElse("<aws-default>")} " +
              s"(region=${cfg.region}, table=${cfg.tableName}): ${sdk.getMessage}"
          )
        case other =>
          ZIO.logError(s"DynamoDB initialisation failed: ${other.getMessage}")
      }
      .refineOrDie {
        case sdk: SdkClientException =>
          new IllegalStateException(
            s"DynamoDB endpoint ${cfg.endpoint.getOrElse("<aws-default>")} is unreachable: ${sdk.getMessage}",
            sdk
          )
        case other => other
      }

  private val serviceLayer: ZLayer[DynamoDb & DynamoDBConfig & ReadinessSignal, Throwable, PersistenceService] =
    ZLayer.fromZIO {
      for
        cfg       <- ZIO.service[DynamoDBConfig]
        dynamo    <- ZIO.service[DynamoDb]
        readiness <- ZIO.service[ReadinessSignal]
        _ <- ZIO.logInfo(
          s"Initialising DynamoDB client: region=${cfg.region} table=${cfg.tableName} endpoint=${cfg.endpoint.getOrElse("<aws-default>")}"
        )
        _ <- ensureTable(dynamo, cfg)
        _ <- readiness.markReady
        _ <- ZIO.logInfo("Persistence ready — readiness signal set")
      yield DynamoDBServiceLive(dynamo, cfg.tableName)
    }

  val live: ZLayer[DynamoDBConfig & ReadinessSignal, Throwable, PersistenceService] =
    ZLayer.makeSome[DynamoDBConfig & ReadinessSignal, PersistenceService](
      NettyHttpClient.default,
      commonAwsConfigLayer,
      AwsConfig.configured(),
      DynamoDb.live,
      serviceLayer
    )
