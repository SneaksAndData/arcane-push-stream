package arcane.ingestion

import arcane.ingestion.BuildInfo
import arcane.ingestion.Models.*
import arcane.ingestion.api.v1.*
import arcane.ingestion.common.LogAspect
import arcane.ingestion.config.AppConfig
import arcane.ingestion.service.*
import com.coralogix.zio.k8s.client.com.sneaksanddata.ingestion.v1alpha1.dataroutes.DataRoutes
import com.coralogix.zio.k8s.client.config.*
import com.coralogix.zio.k8s.client.config.httpclient.*
import com.coralogix.zio.k8s.client.model.K8sNamespace
import zio.*
import zio.http.*
import zio.http.Mode
import zio.http.codec.PathCodec.path
import zio.http.endpoint.openapi.*
import zio.http.netty.NettyConfig

/*----------------------MAIN------------------------*/

object Main extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>>
      Runtime.addLogger(ZLogger.default.map(println(_)).filterLogLevel(_ >= LogLevel.Trace))

  def m: Mode = Mode.current

  private val openAPI =
    OpenAPIGen.fromEndpoints(
      title = "Arcane Ingestion API",
      version = "2.0.0",
      DynamicServer.endpoint,
      HealthEndpoint.endpoint
    )
  private val swaggerRoutes = SwaggerUI.routes("docs" / "openapi", openAPI)

  def run =
    ZIO
      .scoped {
        (for {
          appConfig <- ZIO.service[AppConfig]
          _         <- ZIO.logInfo(s"${BuildInfo.name} ${BuildInfo.version} (${BuildInfo.gitCommit})")
          _         <- ZIO.logInfo(s"ZIO HTTP Mode: $m")
          _ <- ZIO.logInfo(
            s"Starting server on ${appConfig.server.host}:${appConfig.server.port} with ${appConfig.server.nThreads} threads"
          )
          _ <- DynamicRoutingApp.reloader
          _ <- Server.serve(
            swaggerRoutes
              ++ HealthEndpoint.routes
              ++ DynamicServer.routes
          )
        } yield ())
      }
      .onInterrupt(ZIO.logInfo("Shutting down gracefully..."))
      .provide(
        AppConfig.layer,
        k8sDefault,
        DataRoutes.live,
        KubernetesEndpointConfigSourceLive.layer(
          namespace = sys.env.get("RESOURCE_NAMESPACE").map(K8sNamespace(_))
        ),
        ZLayer.fromZIO(ZIO.service[AppConfig].map { cfg =>
          Server.Config.default
            .binding(cfg.server.host, cfg.server.port)
            .gracefulShutdownTimeout(10.seconds)
        }),
        RouteRegistry.live,
        Server.live,
        HealthService.live,
        QueueServiceLive.live
      )
}
