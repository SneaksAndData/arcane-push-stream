package arcane.ingestion.service

import arcane.ingestion.config.{AppConfig, PersistenceProvider}
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
import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

trait PersistenceService:
  def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean]

object PersistenceService:

  /** Persistence provider selector.
    *
    * Materialises exactly one backend based on `AppConfig.persistence`, which is a sealed ADT so the choice is mutually
    * exclusive by construction (no possibility of both backends being configured).
    */
  val live: ZLayer[AppConfig & ReadinessSignal, Throwable, PersistenceService] =
    ZLayer.scoped[AppConfig & ReadinessSignal] {
      for
        cfg <- ZIO.service[AppConfig]
        rs  <- ZIO.service[ReadinessSignal]
        rsLayer = ZLayer.succeed(rs)
        chosen: ZLayer[ReadinessSignal, Throwable, PersistenceService] = cfg.persistence match
          case dynamo: PersistenceProvider.DynamoDB =>
            ZLayer.succeed(dynamo) >>> DynamoDBServiceLive.live
          case mem: PersistenceProvider.InMemory =>
            ZLayer.succeed(mem) >>> InMemoryPersistenceServiceLive.live
        svc <- (rsLayer >>> chosen).build.map(_.get[PersistenceService])
      yield svc
    }

final case class DynamoDBServiceLive(dynamo: DynamoDb, tableName: String) extends PersistenceService:

  def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean] =
    for
      id  <- ZIO.succeed(UUID.randomUUID().toString)
      now <- Clock.instant
      // timestampUTC is a lexicographically-ordered ISO-8601 offset datetime (always UTC) used as the table RANGE key.
      // The downstream arcane-stream-pull plugin polls by `producer = X AND timestampUTC > $lastSeen`,
      // so ordering must match chronological order.
      timestampUTC = OffsetDateTime.ofInstant(now, ZoneOffset.UTC).toString
      // The plugin expects `payload` to be a UTF-8 JSON string (either a single object or an array of objects),
      // not Avro-binary. Callers therefore must POST JSON; we just transcode the bytes verbatim.
      payloadJson = new String(payload, StandardCharsets.UTF_8)
      baseItem = Map(
        AttributeName("producer")     -> AttributeValue(s = Optional.Present(StringAttributeValue(producer))),
        AttributeName("timestampUTC") -> AttributeValue(s = Optional.Present(StringAttributeValue(timestampUTC))),
        AttributeName("id")           -> AttributeValue(s = Optional.Present(StringAttributeValue(id))),
        AttributeName("payload")      -> AttributeValue(s = Optional.Present(StringAttributeValue(payloadJson))),
        AttributeName("createdAt") -> AttributeValue(n =
          Optional.Present(NumberAttributeValue(now.toEpochMilli.toString))
        ),
        AttributeName("schemaSubject") -> AttributeValue(s = Optional.Present(StringAttributeValue(schemaRef.subject))),
        AttributeName("schemaVersion") -> AttributeValue(n =
          Optional.Present(NumberAttributeValue(schemaRef.version.toString))
        )
      )
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

  private val commonAwsConfigLayer: ZLayer[PersistenceProvider.DynamoDB, Nothing, CommonAwsConfig] =
    ZLayer.fromFunction { (cfg: PersistenceProvider.DynamoDB) =>
      CommonAwsConfig(
        region = Some(Region.of(cfg.region)),
        credentialsProvider =
          if cfg.endpoint.isDefined then
            // dynamodb-local accepts any credentials; supply static dummies when overriding the endpoint.
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
              // HASH key: producer identity (caller-provided)
              AttributeDefinition(KeySchemaAttributeName("producer"), ScalarAttributeType.S),
              // RANGE key: ISO-8601 UTC timestamp string. Lexicographic order matches chronological order,
              // so the arcane-stream-pull plugin can poll with `producer = X AND timestampUTC > $lastSeen`.
              AttributeDefinition(KeySchemaAttributeName("timestampUTC"), ScalarAttributeType.S)
            )
          ),
          tableName = TableArn(tableName),
          keySchema = Optional.Present(
            List(
              KeySchemaElement(KeySchemaAttributeName("producer"), KeyType.HASH),
              KeySchemaElement(KeySchemaAttributeName("timestampUTC"), KeyType.RANGE)
            )
          ),
          billingMode = Optional.Present(BillingMode.PAY_PER_REQUEST)
        )
      )
      .mapError(_.toThrowable)
      .unit *> ZIO.logInfo(s"Created DynamoDB table: $tableName")

  /** if the Table doesn't exist and 'autoCreateTable = true', create the table, otherwise throw IllegalStateException
    */
  private def ensureTable(dynamo: DynamoDb, cfg: PersistenceProvider.DynamoDB): IO[Throwable, Unit] =
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

  private val serviceLayer
      : ZLayer[DynamoDb & PersistenceProvider.DynamoDB & ReadinessSignal, Throwable, PersistenceService] =
    ZLayer.fromZIO {
      for
        cfg       <- ZIO.service[PersistenceProvider.DynamoDB]
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

  val live: ZLayer[PersistenceProvider.DynamoDB & ReadinessSignal, Throwable, PersistenceService] =
    ZLayer.makeSome[PersistenceProvider.DynamoDB & ReadinessSignal, PersistenceService](
      NettyHttpClient.default,
      commonAwsConfigLayer,
      AwsConfig.configured(),
      DynamoDb.live,
      serviceLayer
    )
