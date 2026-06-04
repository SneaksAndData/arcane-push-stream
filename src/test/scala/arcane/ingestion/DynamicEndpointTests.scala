package arcane.ingestion

import zio._
import zio.test._
import zio.test.Assertion._
import zio.http._
import arcane.ingestion.api.v1.{EndpointConfig, RouteLoader, RouteRegistry}

object DynamicEndpointTests extends ZIOSpecDefault {

  private val apiVersion = "v1"

  private def post(path: String, body: Body = Body.empty): Request =
    Request.post(URL(Path.root ++ Path(path)), body)

  def spec = suite("Unit tests")(
    test("loader serves configured consumer, rejects others") {
      val routes = RouteLoader.build(apiVersion, List(EndpointConfig("c1")))
      for
        ok   <- routes(post("api/v1/c1/data", Body.fromString("x"))).merge
        miss <- routes(post("api/v1/cX/data")).merge
      yield assertTrue(ok.status == Status.Ok, miss.status == Status.NotFound)
    },
    test("registry swap takes effect immediately") {
      for
        reg <- ZIO.service[RouteRegistry]
        _   <- reg.set(RouteLoader.build(apiVersion, List(EndpointConfig("a"))))
        _   <- reg.set(RouteLoader.build(apiVersion, List(EndpointConfig("b"))))
        rs  <- reg.get
        a   <- rs(post("api/v1/a/data")).merge
        b   <- rs(post("api/v1/b/data")).merge
      yield assertTrue(a.status == Status.NotFound, b.status == Status.Ok)
    }.provideSome[Scope](RouteRegistry.live)
  )
}
