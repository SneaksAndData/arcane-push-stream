package arcane.ingestion.api.v1

import zio._
import zio.http._
import zio.http.Handler
import zio.http.endpoint.*
import zio.http.endpoint._
import zio.http.codec.HttpCodec
import zio.json._
import zio.json.EncoderOps
import arcane.ingestion.service.*
import arcane.ingestion.common.LogAspect

object HealthEndpoint {
  val endpoint = Endpoint(Method.GET / "health")
    .out[Health]
    .tag("system")

  val routes: Routes[HealthService, Response] = Routes(
    endpoint.implementHandler(
      handler(
        (ZIO.logTrace("Hitting health") *>
          ZIO.serviceWithZIO[HealthService](_.health))
          @@ LogAspect.logSpan("get-health")
      )
    )
  )
}
