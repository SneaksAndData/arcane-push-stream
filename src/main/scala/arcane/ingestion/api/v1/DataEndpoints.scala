/*Dynamic POST HTTP endpoints generated from kubernetes CRDs.*/
package arcane.ingestion.api.v1

import zio.*
import zio.http.*
import zio.http.codec.PathCodec.{string, trailing}
import zio.http.endpoint.*
import zio.stream.ZStream
import com.networknt.schema.{InputFormat, JsonSchema, JsonSchemaFactory, SpecVersion}
import scala.jdk.CollectionConverters.*
import scala.util.Try
import arcane.ingestion.service.*
import arcane.ingestion.Models.*
import arcane.ingestion.common.LogAspect
import arcane.ingestion.config.AppConfig

final case class EndpointConfig(producerId: String, payloadSchema: Option[String] = None)

/** Pre-compiles JSON Schema documents and caches the compiled [[JsonSchema]] for fast per-request validation. */
object SchemaCompiler:
  private val factory: JsonSchemaFactory =
    JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)

  /** Compile a JSON Schema document. Returns `Left(error)` if the schema text is malformed. */
  def compile(schemaJson: String): Either[Throwable, JsonSchema] =
    Try(factory.getSchema(schemaJson)).toEither

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

object DynamicServer:

  val endpoint =
    Endpoint(Method.POST / "api" / string("apiVersion") / string("producerId") / "data")
      .in[String]
      .out[String]
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
      endpoints: List[EndpointConfig],
      queueService: QueueService
  ): Routes[Any, Response] =
    Routes
      .fromIterable(endpoints.map(endpointRoute(apiVersion, _)))
      .provideEnvironment(ZEnvironment(queueService))

  private def endpointRoute(apiVersion: String, cfg: EndpointConfig): Route[QueueService, Response] =
    val compiledSchema: Option[JsonSchema] =
      cfg.payloadSchema.flatMap { raw =>
        SchemaCompiler.compile(raw) match
          case Right(s) => Some(s)
          case Left(err) =>
            // Bad schema → log and skip validation for this route rather than failing the rebuild.
            println(s"[RouteLoader] invalid payloadSchema for ${cfg.producerId}: ${err.getMessage}")
            None
      }

    Method.POST / "api" / apiVersion / cfg.producerId / "data" ->
      handler { (req: Request) =>
        (for
          body <- req.body.asString.orDie
          _    <- ZIO.logInfo(s"handling stuff for ${cfg.producerId}")
          _    <- compiledSchema match
                    case None => ZIO.unit
                    case Some(schema) =>
                      val violations = schema.validate(body, InputFormat.JSON).asScala.toList
                      ZIO.when(violations.nonEmpty)(
                        ZIO.fail(
                          Response
                            .text(violations.map(_.getMessage).mkString("; "))
                            .status(Status.BadRequest)
                        )
                      )
          isValid <- ZIO
            .serviceWithZIO[QueueService](_.enqueueToken(body, cfg.producerId))
            .orDieWith(e => new RuntimeException(s"enqueue failed: $e"))
          _ <- ZIO.logInfo(s"enqueue result for ${cfg.producerId}: $isValid")
        yield
          if isValid then Response.text(s"ok for ${cfg.producerId}: $body")
          else Response.status(Status.InternalServerError))
          .merge
          @@ LogAspect.logSpan(cfg.producerId) @@ LogAspect
            .logAnnotateCorrelationId(req)
      }

/*
 * Wiring: watcher → loader → registry.set, all in a forked fiber.
 * The layer that should be injected in the Main app.
 */
object DynamicRoutingApp:
  val reloader: ZIO[RouteRegistry & EndpointConfigSource & AppConfig & QueueService & Scope, Nothing, Unit] =
    for {
      app   <- ZIO.service[AppConfig]
      queue <- ZIO.service[QueueService]
      _ <- EndpointConfigSource.watch
        .mapZIO(cfgs => RouteRegistry.set(RouteLoader.build(app.router.apiVersion, cfgs, queue)))
        .runDrain
        .forkScoped
    } yield ()
