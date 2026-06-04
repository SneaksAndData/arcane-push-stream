package arcane.ingestion

import zio._
import zio.test._
import zio.test.Assertion._
import zio.http._
import arcane.ingestion.api.v1.IngestionEndpoints
import arcane.ingestion.api.v1.HealthEndpoint
import arcane.ingestion.service._

object GreetingServerSpec extends ZIOSpecDefault {

  val allRoutes = IngestionEndpoints.routes ++ HealthEndpoint.routes

  def spec = suite("Unit tests")(
    test("GET /health contains status") {
      for {
        response <- allRoutes.runZIO(Request.get(url"/health"))
        body     <- response.body.asString
      } yield assertTrue(body.contains("status"))
    }.provide(
      Scope.default,
      ZLayer.succeed(HealthServiceLive)
    )
  )
}
