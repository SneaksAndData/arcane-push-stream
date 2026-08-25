package arcane.ingestion.common

import arcane.ingestion.api.v1.{EndpointConfig, RouteLoader, SchemaRef}
import arcane.ingestion.observability.IngestionMetrics
import arcane.ingestion.service.RequestService
import zio.*
import zio.http.*
import zio.test.*

import java.util.concurrent.atomic.AtomicReference

/** Covers what a served request leaves behind in the logs.
  *
  * The thing being protected here is that *every* line belonging to a request names the endpoint that produced it. The
  * access log line is the one that matters most — it is emitted for every request, including ones that matched no route
  * — and it is written from outside the handler, so it is also the one a route-level aspect cannot reach.
  */
object RequestLoggingSpec extends ZIOSpecDefault:

  private val apiVersion            = "v1"
  private val maxContentLengthBytes = 4096L

  private val fakeRequestService: RequestService = new RequestService:
    def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean] =
      ZIO.succeed(true)

  private val noopMetrics: IngestionMetrics = new IngestionMetrics:
    def recordRequest(producer: String, status: String): UIO[Unit]     = ZIO.unit
    def recordIngestionBytes(producer: String, bytes: Long): UIO[Unit] = ZIO.unit
    def recordEndpointReload(activeEndpointCount: Int): UIO[Unit]      = ZIO.unit

  /** One captured log line: its message and the annotations it carried. */
  private final case class Line(message: String, annotations: Map[String, String])

  private def capturingLogger(sink: AtomicReference[Chunk[Line]]): ZLogger[String, Unit] =
    (_, _, _, message: () => String, _, _, _, annotations: Map[String, String]) =>
      sink.updateAndGet(_ :+ Line(message(), annotations))
      ()

  private def serve(request: Request): UIO[(Response, Chunk[Line])] =
    for
      sink <- ZIO.succeed(AtomicReference(Chunk.empty[Line]))
      routes <- RouteLoader.build(
        apiVersion,
        maxContentLengthBytes,
        List(EndpointConfig(producerId = "pt40-injection", schemaSubject = "pt40", schemaVersion = 1)),
        fakeRequestService,
        noopMetrics
      )
      served = routes @@ Middleware.requestLogging() @@ LogAspect.annotateRequestContext
      response <- served
        .run(request)
        .catchAll(failure => ZIO.succeed(failure.merge))
        .provide(Runtime.removeDefaultLoggers ++ Runtime.addLogger(capturingLogger(sink)), Scope.default)
    yield (response, sink.get())

  private def post(path: String): Request =
    Request
      .post(URL(Path.root ++ Path(path)), Body.fromString("{}"))
      .addHeader(Header.Custom("Content-Type", "application/json"))
      .addHeader(Header.ContentLength(2L))

  private val dataPath = "api/v1/pt40-injection/data"

  def spec = suite("request logging")(
    test("the access log line names the endpoint that was called") {
      for (response, lines) <- serve(post(dataPath))
      yield
        val accessLog = lines.find(_.message == "Http request served")
        assertTrue(
          response.status == Status.Accepted,
          accessLog.exists(_.annotations.get("url").contains(s"/$dataPath")),
          accessLog.exists(_.annotations.get("method").contains("POST"))
        )
    },
    test("every line of a request names the endpoint, not just the access log line") {
      for (_, lines) <- serve(post(dataPath))
      yield assertTrue(
        lines.nonEmpty,
        lines.forall(_.annotations.get("url").contains(s"/$dataPath"))
      )
    },
    test("a request that matched no route is still logged with the path that was tried") {
      // the path a caller got wrong is the whole reason to read the log line, so the not-found handler has to be
      // annotated as well
      for (response, lines) <- serve(post("api/v1/unknown-producer/data"))
      yield assertTrue(
        response.status == Status.NotFound,
        lines.exists(line =>
          line.message == "Http request served" &&
            line.annotations.get("url").contains("/api/v1/unknown-producer/data")
        )
      )
    },
    test("the endpoint is named once, not once per spelling") {
      // `requestLogging` annotates its own access log line with `url`/`method`; using different keys here put both
      // spellings of the same fact on that line
      for (_, lines) <- serve(post(dataPath))
      yield
        val accessLog = lines.find(_.message == "Http request served")
        assertTrue(
          lines.count(_.message == "Http request served") == 1,
          accessLog.exists(_.annotations.keySet.count(_.toLowerCase.contains("url")) == 1),
          accessLog.exists(_.annotations.keySet.count(_.toLowerCase.contains("method")) == 1),
          accessLog.exists(!_.annotations.contains("http-path")),
          accessLog.exists(!_.annotations.contains("http-method"))
        )
    },
    test("all lines of one request share a single correlation id") {
      // resolving the id per aspect rather than once per request used to mint a second uuid, splitting the handler's
      // lines away from the access log line they belong to
      for (_, lines) <- serve(post(dataPath))
      yield assertTrue(lines.flatMap(_.annotations.get("correlation-id")).toSet.size == 1)
    },
    test("the caller's own correlation id is used when it supplies one") {
      for (_, lines) <- serve(post(dataPath).addHeader(Header.Custom("X-Correlation-ID", "trace-42")))
      yield assertTrue(lines.forall(_.annotations.get("correlation-id").contains("trace-42")))
    },
    test("the client ip is resolved from the proxy header the ingress sets") {
      for (_, lines) <- serve(post(dataPath).addHeader(Header.Custom("X-Forwarded-For", "203.0.113.7, 10.0.0.1")))
      yield assertTrue(lines.forall(_.annotations.get("client-ip").contains("203.0.113.7")))
    }
  )
