package arcane.ingestion

import zio._
import zio.test._
import zio.test.Assertion._
import zio.http._
import arcane.ingestion.api.v1.{
  EndpointConfig,
  IcebergColumnSpec,
  IcebergTableSpec,
  RouteLoader,
  RouteRegistry,
  SchemaRef
}
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

  private val failingRequestService: RequestService = new RequestService:
    def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean] =
      ZIO.fail:
        Throwable("Connection Error")

  // Test double: metric emission is a no-op — we assert route behavior, not observability side-effects.
  private val noopMetrics: IngestionMetrics = new IngestionMetrics:
    def recordRequest(producer: String, status: String): UIO[Unit]     = ZIO.unit
    def recordIngestionBytes(producer: String, bytes: Long): UIO[Unit] = ZIO.unit
    def recordEndpointReload(activeEndpointCount: Int): UIO[Unit]      = ZIO.unit

  private def post(path: String, payload: String = "x"): Request =
    Request
      .post(URL(Path.root ++ Path(path)), Body.fromString(payload))
      .addHeader(Header.Custom("Content-Type", "application/json"))
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
      yield assertTrue(ok.status == Status.Accepted, miss.status == Status.NotFound)
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
      yield assertTrue(a.status == Status.NotFound, b.status == Status.Accepted)
    }.provide(RouteRegistry.live, Scope.default),
    test("rejects requests whose Content-Length exceeds the limit") {
      val oversize = maxContentLengthBytes + 1
      val req = Request
        .post(URL(Path.root ++ Path("api/v1/c1/data")), Body.fromString("x"))
        .addHeader(Header.Custom("Content-Type", "application/json"))
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
      val req = Request
        .post(URL(Path.root ++ Path("api/v1/c1/data")), Body.fromString("x"))
        .addHeader(Header.Custom("Content-Type", "application/json"))
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
        res.status == Status.Accepted,
        queue.get("enq-test").map(new String(_, java.nio.charset.StandardCharsets.UTF_8)).contains(body)
      )
    },
    test("rejects non-json Content-Type") {
      val req = Request
        .post(URL(Path.root ++ Path("api/v1/c1/data")), Body.fromString("""{"hello":"world"}"""))
        .addHeader(Header.Custom("Content-Type", "text/plain"))
        .addHeader(Header.ContentLength(17L))
      for
        routes <- build("c1")
        res    <- routes.run(req)
        body   <- res.body.asString
      yield assertTrue(
        res.status == Status.UnsupportedMediaType,
        body == "Only application/json is accepted"
      )
    },
    test("accepts application/json Content-Type with charset parameter") {
      val req = Request
        .post(URL(Path.root ++ Path("api/v1/c1/data")), Body.fromString("""{"hello":"world"}"""))
        .addHeader(Header.Custom("Content-Type", "application/json; charset=utf-8"))
        .addHeader(Header.ContentLength(17L))
      for
        routes <- build("c1")
        res    <- routes.run(req)
      yield assertTrue(res.status == Status.Accepted)
    },
    test("Avro-bound route validates JSON, persists the raw JSON, and forwards SchemaRef") {
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
        okRes.status == Status.Accepted,
        // The schema is used for validation only — the persisted payload stays raw UTF-8 JSON.
        queue.get("avro-test").map(new String(_, java.nio.charset.StandardCharsets.UTF_8)).contains(validJson),
        schemas
          .get("avro-test")
          .exists(r => r.subject == "orders" && r.version == 3 && r.fingerprint.exists(_.nonEmpty)),
        badRes.status == Status.BadRequest
      )
    },
    test("persists identical bytes with and without a payload schema") {
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
      val payload = """{"id":"o-1","amount":42}"""
      val schemaBound = EndpointConfig(
        producerId = "parity-schema",
        schemaSubject = "orders",
        schemaVersion = 3,
        payloadSchema = Some(avroSchema)
      )
      val schemaless = EndpointConfig(
        producerId = "parity-plain",
        schemaSubject = "orders",
        schemaVersion = 3,
        payloadSchema = None
      )
      for
        _ <- ZIO.succeed(queue.clear())
        _ <- ZIO.succeed(schemas.clear())
        routes <- RouteLoader.build(
          apiVersion,
          maxContentLengthBytes,
          List(schemaBound, schemaless),
          fakeRequestService,
          noopMetrics
        )
        withRes   <- routes.run(post(s"api/v1/${schemaBound.producerId}/data", payload))
        plainRes  <- routes.run(post(s"api/v1/${schemaless.producerId}/data", payload))
        withBody  <- withRes.body.asString
        plainBody <- plainRes.body.asString
        withBytes  = queue.get(schemaBound.producerId)
        plainBytes = queue.get(schemaless.producerId)
      yield assertTrue(
        withRes.status == Status.Accepted,
        plainRes.status == Status.Accepted,
        // Sanity check: the schema really was compiled and bound, so the parity below is meaningful
        // rather than a silent fallback to the "no validation" path.
        schemas.get(schemaBound.producerId).exists(_.fingerprint.exists(_.nonEmpty)),
        schemas.get(schemaless.producerId).exists(_.fingerprint.isEmpty),
        withBytes.map(new String(_, java.nio.charset.StandardCharsets.UTF_8)).contains(payload),
        plainBytes.map(new String(_, java.nio.charset.StandardCharsets.UTF_8)).contains(payload),
        withBytes.zip(plainBytes).exists((a, b) => a.sameElements(b)),
        withBody == plainBody.replace(schemaless.producerId, schemaBound.producerId)
      )
    },
    test("persists only the document the jsonExpressionPointer selects") {
      val avroSchema =
        """{
          |  "type": "record",
          |  "name": "Order",
          |  "namespace": "test",
          |  "fields": [
          |    { "name": "id", "type": "string" },
          |    {
          |      "name": "payload",
          |      "type": {
          |        "type": "record",
          |        "name": "OrderPayload",
          |        "namespace": "test",
          |        "fields": [ { "name": "amount", "type": "int" } ]
          |      }
          |    }
          |  ]
          |}""".stripMargin
      val cfg = EndpointConfig(
        producerId = "pointer-test",
        schemaSubject = "orders",
        schemaVersion = 3,
        payloadSchema = Some(avroSchema),
        jsonExpressionPointer = Some("/payload")
      )
      for
        _      <- ZIO.succeed(queue.clear())
        routes <- RouteLoader.build(apiVersion, maxContentLengthBytes, List(cfg), fakeRequestService, noopMetrics)
        // the envelope is validated against the full schema, then dropped: the stored document has to line up
        // with the hoisted iceberg columns, which are derived from the pointed-at record alone
        res <- routes.run(post("api/v1/pointer-test/data", """{"id":"o-1","payload":{"amount":42}}"""))
      yield assertTrue(
        res.status == Status.Accepted,
        queue
          .get("pointer-test")
          .map(new String(_, java.nio.charset.StandardCharsets.UTF_8))
          .contains("""{"amount":42}""")
      )
    },
    test("rejects a payload the jsonExpressionPointer does not resolve against") {
      val cfg = EndpointConfig(
        producerId = "pointer-missing",
        schemaSubject = "orders",
        schemaVersion = 3,
        payloadSchema = None,
        jsonExpressionPointer = Some("/payload")
      )
      for
        _      <- ZIO.succeed(queue.clear())
        routes <- RouteLoader.build(apiVersion, maxContentLengthBytes, List(cfg), fakeRequestService, noopMetrics)
        res    <- routes.run(post("api/v1/pointer-missing/data", """{"id":"o-1"}"""))
        body   <- res.body.asString
      yield assertTrue(
        res.status == Status.BadRequest,
        body == "application error: invalid json path for payload: '/payload'",
        // nothing is stored, so a misconfigured route cannot quietly fill the table with unusable documents
        queue.get("pointer-missing").isEmpty
      )
    },
    test("renames a root id on an iceberg-bound route so the document matches the column") {
      val cfg = EndpointConfig(
        producerId = "rename-test",
        schemaSubject = "orders",
        schemaVersion = 3,
        payloadSchema = None,
        iceberg = Some(
          IcebergTableSpec(
            catalogUri = "http://localhost:20001/catalog",
            warehouse = "lakehouse-bronze",
            namespace = "arcane_pull_test",
            tableName = "events",
            columns = Seq(IcebergColumnSpec("push_event_id", "string"))
          )
        )
      )
      val plain = cfg.copy(producerId = "rename-none", iceberg = None)
      for
        _ <- ZIO.succeed(queue.clear())
        routes <- RouteLoader.build(
          apiVersion,
          maxContentLengthBytes,
          List(cfg, plain),
          fakeRequestService,
          noopMetrics
        )
        _ <- routes.run(post("api/v1/rename-test/data", """{"id":"o-1","amount":42}"""))
        _ <- routes.run(post("api/v1/rename-none/data", """{"id":"o-1","amount":42}"""))
      yield assertTrue(
        queue
          .get("rename-test")
          .map(new String(_, java.nio.charset.StandardCharsets.UTF_8))
          .contains("""{"push_event_id":"o-1","amount":42}"""),
        // a route without a target table has no column to match, so the producer's names are left as sent
        queue
          .get("rename-none")
          .map(new String(_, java.nio.charset.StandardCharsets.UTF_8))
          .contains("""{"id":"o-1","amount":42}""")
      )
    },
    test("Returns 500 if DynamoDB not available") {
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
      val validJson = """{"id":"o-1","amount":42}"""
      val cfg = EndpointConfig(
        producerId = "avro-test",
        schemaSubject = "orders",
        schemaVersion = 3,
        payloadSchema = Some(avroSchema)
      )
      for
        _      <- ZIO.succeed(queue.clear())
        _      <- ZIO.succeed(schemas.clear())
        routes <- RouteLoader.build(apiVersion, maxContentLengthBytes, List(cfg), failingRequestService, noopMetrics)
        res    <- routes.run(post("api/v1/avro-test/data", validJson))
      yield assertTrue(
        res.status == Status.InternalServerError
      )
    }
    // `queue`/`schemas` are shared mutable test doubles that individual tests clear before use,
    // so the tests must not run concurrently.
  ) @@ TestAspect.sequential
}
