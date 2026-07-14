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

object ReadinessEndpoint {
  val endpoint = Endpoint(Method.GET / "ready")
    .out[String](Status.Ok)
    .outError[String](Status.ServiceUnavailable)
    .tag("system")

  val routes: Routes[ReadinessSignal, Response] = Routes(
    endpoint.implementHandler(
      handler(
        (for
          ready <- ReadinessSignal.isReady
          out   <- if ready then ZIO.succeed("ready") else ZIO.fail("not ready")
        yield out)
          @@ LogAspect.logSpan("get-ready")
      )
    )
  )
}

object TeapotEndpoint {
  val endpoint = Endpoint(Method.GET / "teapot")
    .out[Unit](Status.Custom(418, "I'm a teapot"))
    .tag("system")

  val routes: Routes[Any, Response] = Routes(
    Method.GET / "teapot" -> handler(
      ZIO.succeed(Response.status(Status.Custom(418, "I'm a teapot")))
        @@ LogAspect.logSpan("get-teapot")
    )
  )
}
