/*Dynamic POST HTTP endpoints generated from kubernetes CRDs.*/
package arcane.ingestion.api.v1

import zio.*
import zio.http.*
import zio.http.codec.{HttpCodec, PathCodec}
import zio.http.codec.PathCodec.{string, trailing}
import zio.http.endpoint.*
import zio.stream.ZStream
import scala.util.Try
import arcane.ingestion.service.*
import arcane.ingestion.Models.*
import arcane.ingestion.common.LogAspect
import arcane.ingestion.config.AppConfig
import arcane.ingestion.observability.IngestionMetrics

final case class EndpointConfig(
    producerId: String,
    schemaSubject: String,
    schemaVersion: Int,
    payloadSchema: Option[String] = None,
    iceberg: Option[IcebergTableSpec] = None
)

/** Iceberg target table description sourced from a `DataRoute` CRD.
  *
  * The [[arcane.ingestion.service.IcebergProvisioner]] uses this to ensure the referenced table exists at startup.
  * Field-ids are assigned positionally (1..n in the order columns are declared); reordering the columns of an existing
  * table is therefore considered a breaking change.
  */
final case class IcebergTableSpec(
    catalogUri: String,
    warehouse: String,
    namespace: String,
    tableName: String,
    columns: Seq[IcebergColumnSpec],
    initialProperties: Map[String, String] = Map.empty
)

final case class IcebergColumnSpec(
    name: String,
    `type`: String,
    required: Boolean = false
)

/** Identity of the writer schema bound to a route, persisted alongside every record so the downstream parquet writer
  * can resolve the writer schema. `subject` and `version` come from the CRD (always present). `fingerprint` is the
  * CRC-64-AVRO parsing fingerprint of the compiled Avro schema; absent for routes without a `payloadSchema`.
  */
final case class SchemaRef(subject: String, version: Int, fingerprint: Option[String])

/** Pre-compiles Avro schema documents and caches the compiled [[CompiledAvroSchema]] for fast per-request validation.
  */
object SchemaCompiler:
  /** Compile an Avro schema document. Returns `Left(error)` if the schema text is malformed. */
  def compile(schemaJson: String): Either[Throwable, CompiledAvroSchema] =
    AvroSchemaCompiler.compile(schemaJson)

/** Live source of the dynamic endpoint list.
  *
  * Abstracts over *where* the route list comes from. Emits a snapshot on subscribe and every subsequent update;
  * implementations should dedup identical successive snapshots.
  */
trait EndpointConfigSource:
  def watch: ZStream[Any, Nothing, List[EndpointConfig]]

object EndpointConfigSource:
  def watch: ZStream[EndpointConfigSource, Nothing, List[EndpointConfig]] =
    ZStream.serviceWithStream[EndpointConfigSource](_.watch)

/** Thread-safe, atomically-swappable holder of the active route table. */
trait RouteRegistry:
  def get: UIO[Routes[Any, Response]]
  def set(routes: Routes[Any, Response]): UIO[Unit]

object RouteRegistry:
  val live: ULayer[RouteRegistry] =
    ZLayer.fromZIO:
      Ref
        .make[Routes[Any, Response]](Routes.empty)
        .map: ref =>
          new RouteRegistry:
            def get: UIO[Routes[Any, Response]]               = ref.get
            def set(routes: Routes[Any, Response]): UIO[Unit] = ref.set(routes)

  def get: URIO[RouteRegistry, Routes[Any, Response]] =
    ZIO.serviceWithZIO[RouteRegistry](_.get)
  def set(routes: Routes[Any, Response]): URIO[RouteRegistry, Unit] =
    ZIO.serviceWithZIO[RouteRegistry](_.set(routes))

/** Data endpoint declaration, primarily used for OpenAPI doc generation.
  */
object DynamicServer:

  val endpoint =
    Endpoint(Method.POST / "api" / string("apiVersion") / string("producerId") / "data")
      .in[String]
      .out[String](Status.Accepted)
      .outErrors[AppError](
        HttpCodec.error[SerializationError](Status.BadRequest),
        HttpCodec.error[SchemaValidationError](Status.BadRequest),
        HttpCodec.error[NoContentError](Status.BadRequest),
        HttpCodec.error[ContentTypeError](Status.UnsupportedMediaType),
        HttpCodec.error[LengthRequiredError](Status.LengthRequired),
        HttpCodec.error[ConentLengthTooLargeError](Status.RequestEntityTooLarge),
        HttpCodec.error[ConnectionError](Status.InternalServerError),
        HttpCodec.error[DataWriteError](Status.InternalServerError)
      )
      .tag("user")

  val routes: Routes[RouteRegistry, Response] =
    Routes(
      Method.POST / "api" / trailing -> handler { (_: Path, req: Request) =>
        ZIO.scoped(RouteRegistry.get.flatMap(_.apply(req).merge))
      }
    )

/** Pure builder that turns a list of [[EndpointConfig]] into ready-to-serve [[Routes]]. */
object RouteLoader:
  def build(
      apiVersion: String,
      maxContentLengthBytes: Long,
      endpoints: List[EndpointConfig],
      queueService: RequestService,
      metrics: IngestionMetrics
  ): UIO[Routes[Any, Response]] =
    ZIO
      .foreach(endpoints) { cfg =>
        val baseRef = SchemaRef(cfg.schemaSubject, cfg.schemaVersion, fingerprint = None)
        cfg.payloadSchema match
          case None =>
            // No Avro schema bound → no validation, but the SchemaRef from the CRD is still
            // attached to every record so the downstream router knows where the bytes belong.
            ZIO.succeed(cfg -> (Option.empty[CompiledAvroSchema], baseRef))
          case Some(raw) =>
            SchemaCompiler.compile(raw) match
              case Right(s) =>
                ZIO.succeed(cfg -> (Some(s), baseRef.copy(fingerprint = Some(s.fingerprint))))
              case Left(err) =>
                ZIO
                  .logWarning(
                    s"[RouteLoader] invalid Avro payloadSchema for ${cfg.producerId}: ${err.getMessage} — disabling validation for this route"
                  )
                  .as(cfg -> (Option.empty[CompiledAvroSchema], baseRef))
      }
      .map { compiled =>
        Routes
          .fromIterable(
            compiled.map { case (cfg, (schema, ref)) =>
              endpointRoute(apiVersion, maxContentLengthBytes, cfg, schema, ref)
            }
          )
          .provideEnvironment(ZEnvironment(queueService, metrics))
      }

  private def appErrorToResponse(maxContentLengthBytes: Long)(err: AppError): Response = err match
    case SerializationError(c)    => Response.text(s"Invalid payload: $c").status(Status.BadRequest)
    case SchemaValidationError(c) => Response.text(s"Schema validation failed: $c").status(Status.BadRequest)
    case ConnectionError(c) => Response.text(s"Persistence connection error: $c").status(Status.InternalServerError)
    case DataWriteError(c)  => Response.text(s"Persistence write error: $c").status(Status.InternalServerError)
    case ConentLengthTooLargeError(l) =>
      Response.text(s"Payload too large: $l bytes (max $maxContentLengthBytes)").status(Status.RequestEntityTooLarge)
    case LengthRequiredError()   => Response.text("Content-Length header is required").status(Status.LengthRequired)
    case NoContentError()        => Response.text("Request body is required").status(Status.BadRequest)
    case ContentEncodingError(t) => Response.text(t).status(Status.BadRequest)
    case ContentTypeError()  => Response.text("Only application/json is accepted").status(Status.UnsupportedMediaType)
    case AccessDeniedError() => Response.status(Status.Forbidden)
    case ParseError()        => Response.text("Parse error").status(Status.BadRequest)

  private def classifyPersistenceError(t: Throwable): AppError =
    DataWriteError(Option(t.getMessage).getOrElse(t.getClass.getSimpleName))

  private def isApplicationJsonContentType(req: Request): Boolean =
    req.rawHeader(Header.ContentType).flatMap(MediaType.forContentType).exists { mediaType =>
      mediaType.mainType.equalsIgnoreCase("application") && mediaType.subType.equalsIgnoreCase("json")
    }

  /* blueprint for the data ingestion endpoint.
   *
   * Validate the request to the associated schema (CRD for now) and save the payload to persistent storage.
   *
   * Throws:
   * - SerializationError    - invalid payload                               - 400 BadRequest
   * - SchemaValidationError - payload validation failed                     - 400 BadRequest (with reason)
   * - ConnectionError       - e.g.: AWSClient lost connection               - 500 InternalServerError
   * - DataWriteError        - Could not persist the data (AwsDynamoDBError) - 500 InternalServerError
   * - 411 Length Required (Content-Length header is mandatory)
   * - 413 content Too Large (Content larger than 400kB)
   * */
  private def endpointRoute(
      apiVersion: String,
      maxContentLengthBytes: Long,
      cfg: EndpointConfig,
      compiledSchema: Option[CompiledAvroSchema],
      schemaRef: SchemaRef
  ): Route[RequestService & IngestionMetrics, Response] =
    Method.POST / "api" / apiVersion / cfg.producerId / "data" ->
      handler { (req: Request) =>
        val handled: ZIO[RequestService & IngestionMetrics, AppError, Response] =
          for
            _ <- ZIO
              .fail(ContentTypeError())
              .unless(isApplicationJsonContentType(req))
            contentLength <- ZIO
              .fromOption(req.header(Header.ContentLength).map(_.length))
              .orElseFail(LengthRequiredError())
            _ <- ZIO
              .fail(ConentLengthTooLargeError(contentLength))
              .when(contentLength > maxContentLengthBytes)
            _ <- ZIO
              .fail(NoContentError())
              .when(contentLength <= 0)
            body <- req.body.asString.mapError(e => SerializationError(e.getMessage))
            _ <- ZIO
              .fail(NoContentError())
              .when(body.isEmpty)
            // Avro path: validate by decoding JSON against the schema, then re-encode as binary.
            // No schema configured: persist the raw JSON bytes unchanged.
            payloadBytes <- compiledSchema match
              case None =>
                ZIO.succeed(body.getBytes(java.nio.charset.StandardCharsets.UTF_8))
              case Some(schema) =>
                ZIO
                  .fromEither(schema.validateAndEncode(body))
                  .mapError(t => SchemaValidationError(Option(t.getMessage).getOrElse(t.getClass.getSimpleName)))
            isValid <- ZIO
              .serviceWithZIO[RequestService](_.enqueueToken(payloadBytes, cfg.producerId, schemaRef))
              .mapError(classifyPersistenceError)
            _ <- IngestionMetrics.recordIngestionBytes(cfg.producerId, payloadBytes.length.toLong)
            _ <- ZIO.logInfo(
              s"enqueue result for ${cfg.producerId}: $isValid (${payloadBytes.length} bytes, " +
                s"schema=${schemaRef.subject}:v${schemaRef.version})"
            )
          yield
            if isValid then
              Response.text(s"accepted for ${cfg.producerId}: ${payloadBytes.length} bytes").status(Status.Accepted)
            else Response.status(Status.InternalServerError)

        handled
          .tap(resp => IngestionMetrics.recordRequest(cfg.producerId, if resp.status.isSuccess then "ok" else "error"))
          .catchAll { err =>
            IngestionMetrics
              .recordRequest(cfg.producerId, "error")
              .as(appErrorToResponse(maxContentLengthBytes)(err))
          }
          @@ LogAspect.logSpan(cfg.producerId) @@ LogAspect.logAnnotateCorrelationId(req)
      }

/* Service to watch kubernetes CRD resource and on change
 * - mount the HTTP endpoint  using `RouteLoader`
 * - procure tables based on provisioner configuration (e.g. `IcebergProvisioner`)
 *
 * Wiring: watcher → loader → registry.set, all in a forked fiber.
 */
object DynamicRoutingApp:
  val reloader: ZIO[
    RouteRegistry & EndpointConfigSource & AppConfig & RequestService & IcebergProvisioner & IngestionMetrics & Scope,
    Nothing,
    Unit
  ] =
    for {
      app         <- ZIO.service[AppConfig]
      queue       <- ZIO.service[RequestService]
      provisioner <- ZIO.service[IcebergProvisioner]
      metrics     <- ZIO.service[IngestionMetrics]
      // Track producers we've already attempted to provision so subsequent CRD updates
      // (Modified events that don't change the iceberg block) don't re-issue catalog calls.
      // The framework's createTable is itself idempotent, but skipping known-good specs
      // keeps the logs clean and avoids hammering the catalog REST endpoint.
      seen <- Ref.make(Set.empty[String])
      _ <- EndpointConfigSource.watch
        .mapZIO(cfgs =>
          provisionNewTables(provisioner, seen, cfgs) *>
            RouteLoader
              .build(app.router.apiVersion, app.server.maxContentLengthBytes, cfgs, queue, metrics)
              .flatMap(RouteRegistry.set) *>
            metrics.recordEndpointReload(cfgs.size)
        )
        .runDrain
        .forkScoped
    } yield ()

  /** For each endpoint whose CRD declares an iceberg table, ensure the table exists via [[IcebergProvisioner]].
    * Failures are logged and swallowed — to prevent bad CR take down the route reloader fiber.
    */
  private def provisionNewTables(
      provisioner: IcebergProvisioner,
      seen: Ref[Set[String]],
      cfgs: List[EndpointConfig]
  ): UIO[Unit] =
    ZIO.foreachDiscard(cfgs) { cfg =>
      cfg.iceberg match
        case None => ZIO.unit
        case Some(spec) =>
          for
            alreadySeen <- seen.modify(s => (s.contains(cfg.producerId), s + cfg.producerId))
            _ <- ZIO
              .unless(alreadySeen) {
                provisioner
                  .provision(spec)
                  .tapError(e =>
                    ZIO.logWarningCause(
                      s"[DynamicRoutingApp] iceberg provisioning failed for ${cfg.producerId}: ${e.getMessage}",
                      Cause.fail(e)
                    )
                  )
                  .ignore
              }
              .unit
          yield ()
    }
