package arcane.ingestion

import zio._
import zio.test._
import zio.test.Assertion._
import zio.http._
import zio.json._
import arcane.ingestion.api.v1.IngestionEndpoints
import arcane.ingestion.api.v1.HealthEndpoint
import arcane.ingestion.service._

object IntegrationTestServerSpec extends ZIOSpecDefault {

  val routes = IngestionEndpoints.routes ++ HealthEndpoint.routes

  def spec = suite("Integration tests")(
    test("GET /health status is OK") {
      for {
        _        <- TestRandom.setSeed(123L)
        client   <- ZIO.service[Client]
        port     <- ZIO.serviceWithZIO[Server](_.port)
        _        <- TestServer.addRoutes(routes)
        response <- client(Request.get(URL.root.port(port) / "health"))
        body     <- response.body.asString
      } yield assertTrue(body == Health("ok", 1087885590).toJson)
    }.provide(
      TestServer.layer,
      Client.default,
      Scope.default,
      Driver.default,
      ZLayer.succeed(Server.Config.default.onAnyOpenPort),
      ZLayer.succeed(HealthServiceLive)
    )
  )
}
