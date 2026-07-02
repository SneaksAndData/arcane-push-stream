package arcane.ingestion

import zio._
import zio.test._
import zio.test.Assertion._
import zio.http._
import arcane.ingestion.api.v1.{EndpointConfig, RouteLoader, RouteRegistry, SchemaRef}
import arcane.ingestion.observability.IngestionMetrics
import arcane.ingestion.service.RequestService

import scala.collection.mutable

object DynamicEndpointTests extends ZIOSpecDefault {

  private val apiVersion            = "v1"
  private val maxContentLengthBytes = 4096L

  private val queue   = mutable.Map.empty[String, Array[Byte]]
  private val schemas = mutable.Map.empty[String, SchemaRef]

  private val fakeRequestService: RequestService = new RequestService:
    def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean] =
      ZIO.succeed:
        queue.update(producer, payload)
        schemas.update(producer, schemaRef)
        true

  // Test double: metric emission is a no-op — we assert route behavior, not observability side-effects.
  private val noopMetrics: IngestionMetrics = new IngestionMetrics:
    def recordRequest(producer: String, status: String): UIO[Unit]        = ZIO.unit
    def recordIngestionBytes(producer: String, bytes: Long): UIO[Unit]    = ZIO.unit
    def recordEndpointReload(activeEndpointCount: Int): UIO[Unit]         = ZIO.unit

  private def post(path: String, payload: String = "x"): Request =
    Request
      .post(URL(Path.root ++ Path(path)), Body.fromString(payload))
      .addHeader(Header.ContentLength(payload.length.toLong))

  private def cfg(producer: String, version: Int = 1): EndpointConfig =
    EndpointConfig(
      producerId = producer,
      schemaSubject = s"$producer-subject",
      schemaVersion = version
    )

  private def build(producers: String*): UIO[Routes[Any, Response]] =
    RouteLoader.build(
      apiVersion,
      maxContentLengthBytes,
      producers.map(cfg(_)).toList,
      fakeRequestService,
      noopMetrics
    )

  def spec = suite("Unit tests")(
    test("loader serves configured consumer, rejects others") {
      for
        routes <- build("c1")
        ok     <- routes.run(post("api/v1/c1/data"))
        miss   <- routes.run(post("api/v1/cX/data"))
      yield assertTrue(ok.status == Status.Ok, miss.status == Status.NotFound)
    },
    test("registry swap takes effect immediately") {
      for
        reg <- ZIO.service[RouteRegistry]
        rsA <- build("a")
        _   <- reg.set(rsA)
        rsB <- build("b")
        _   <- reg.set(rsB)
        rs  <- reg.get
        a   <- rs.run(post("api/v1/a/data"))
        b   <- rs.run(post("api/v1/b/data"))
      yield assertTrue(a.status == Status.NotFound, b.status == Status.Ok)
    }.provide(RouteRegistry.live, Scope.default),
    test("rejects requests whose Content-Length exceeds the limit") {
      val oversize = maxContentLengthBytes + 1
      val req = Request
        .post(URL(Path.root ++ Path("api/v1/c1/data")), Body.fromString("x"))
        .addHeader(Header.ContentLength(oversize))
      for
        routes <- build("c1")
        res    <- routes.run(req)
        body   <- res.body.asString
      yield assertTrue(
        res.status == Status.RequestEntityTooLarge,
        body == s"Payload too large: $oversize bytes (max $maxContentLengthBytes)"
      )
    },
    test("rejects requests without a Content-Length header") {
      val req = Request.post(URL(Path.root ++ Path("api/v1/c1/data")), Body.fromString("x"))
      for
        routes <- build("c1")
        res    <- routes.run(req)
        body   <- res.body.asString
      yield assertTrue(
        res.status == Status.LengthRequired,
        body == "Content-Length header is required"
      )
    },
    test("successful request enqueues the payload into the queue") {
      val body = """{"hello":"world"}"""
      for
        _      <- ZIO.succeed(queue.clear())
        routes <- build("enq-test")
        res    <- routes.run(post("api/v1/enq-test/data", body))
      yield assertTrue(
        res.status == Status.Ok,
        queue.get("enq-test").map(new String(_, java.nio.charset.StandardCharsets.UTF_8)).contains(body)
      )
    },
    test("Avro-bound route validates JSON, encodes to binary, and forwards SchemaRef") {
      val avroSchema =
        """{
          |  "type": "record",
          |  "name": "Order",
          |  "namespace": "test",
          |  "fields": [
          |    { "name": "id",     "type": "string" },
          |    { "name": "amount", "type": "int"    }
          |  ]
          |}""".stripMargin
      val validJson   = """{"id":"o-1","amount":42}"""
      val invalidJson = """{"id":"o-1","amount":"not-a-number"}"""
      val cfg = EndpointConfig(
        producerId = "avro-test",
        schemaSubject = "orders",
        schemaVersion = 3,
        payloadSchema = Some(avroSchema)
      )
      for
        _      <- ZIO.succeed(queue.clear())
        _      <- ZIO.succeed(schemas.clear())
        routes <- RouteLoader.build(apiVersion, maxContentLengthBytes, List(cfg), fakeRequestService, noopMetrics)
        okRes  <- routes.run(post("api/v1/avro-test/data", validJson))
        badRes <- routes.run(post("api/v1/avro-test/data", invalidJson))
      yield assertTrue(
        okRes.status == Status.Ok,
        // Avro binary is smaller than the JSON it was decoded from.
        queue.get("avro-test").exists(b => b.length > 0 && b.length < validJson.length),
        schemas
          .get("avro-test")
          .exists(r => r.subject == "orders" && r.version == 3 && r.fingerprint.exists(_.nonEmpty)),
        badRes.status == Status.BadRequest
      )
    }
  )
}
