package arcane.ingestion

import zio._
import zio.test._
import zio.test.Assertion._
import zio.http._
import arcane.ingestion.api.v1.{EndpointConfig, RouteLoader, RouteRegistry}
import arcane.ingestion.service.RequestService

import scala.collection.mutable

object DynamicEndpointTests extends ZIOSpecDefault {

  private val apiVersion            = "v1"
  private val maxContentLengthBytes = 4096L

  private val queue = mutable.Map.empty[String, String]

  private val fakeRequestService: RequestService = new RequestService:
    def enqueueToken(payload: String, producer: String): IO[Throwable, Boolean] =
      ZIO.succeed:
        queue.update(producer, payload)
        true

  private def post(path: String, payload: String = "x"): Request =
    Request
      .post(URL(Path.root ++ Path(path)), Body.fromString(payload))
      .addHeader(Header.ContentLength(payload.length.toLong))

  private def build(producers: String*): UIO[Routes[Any, Response]] =
    RouteLoader.build(
      apiVersion,
      maxContentLengthBytes,
      producers.map(EndpointConfig(_)).toList,
      fakeRequestService
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
      for
        _      <- ZIO.succeed(queue.clear())
        routes <- build("enq-test")
        res    <- routes.run(post("api/v1/enq-test/data", """{"hello":"world"}"""))
      yield assertTrue(
        res.status == Status.Ok,
        queue.get("enq-test").contains("""{"hello":"world"}""")
      )
    }
  )
}
