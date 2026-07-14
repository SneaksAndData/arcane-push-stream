package arcane.ingestion

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.http.*
import zio.json.*
import arcane.ingestion.api.v1.{HealthEndpoint, TeapotEndpoint}
import arcane.ingestion.service.*

object IntegrationTestServerSpec extends ZIOSpecDefault {

  given JsonDecoder[Health] = DeriveJsonDecoder.gen[Health]

  val routes = HealthEndpoint.routes ++ TeapotEndpoint.routes

  def spec = suite("Integration tests")(
    test("GET /health status is OK") {
      for {
        client   <- ZIO.service[Client]
        port     <- ZIO.serviceWithZIO[Server](_.port)
        _        <- TestServer.addRoutes(routes)
        response <- client(Request.get(URL.root.port(port) / "health"))
        body     <- response.body.asString
        parsed   <- ZIO.fromEither(body.fromJson[Health]).mapError(new RuntimeException(_))
      } yield assertTrue(
        response.status == Status.Ok,
        parsed.status == "ok"
      )
    },
    test("GET /teapot") {
      for {
        client   <- ZIO.service[Client]
        port     <- ZIO.serviceWithZIO[Server](_.port)
        _        <- TestServer.addRoutes(routes)
        response <- client(Request.get(URL.root.port(port) / "teapot"))
      } yield assertTrue(
        response.status.code == 418
      )
    }
  ).provide(
    TestServer.layer,
    Client.default,
    Scope.default,
    Driver.default,
    ZLayer.succeed(Server.Config.default.onAnyOpenPort),
    HealthService.live
  )
}
