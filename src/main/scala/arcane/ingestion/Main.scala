package arcane.ingestion

import arcane.ingestion.BuildInfo
import arcane.ingestion.Models.*
import arcane.ingestion.api.v1.*
import arcane.ingestion.common.LogAspect
import arcane.ingestion.config.AppConfig
import arcane.ingestion.observability.{IngestionMetrics, ObservabilityLayers}
import arcane.ingestion.service.*
import com.coralogix.zio.k8s.client.com.sneaksanddata.ingestion.v1alpha1.dataroutes.DataRoutes
import com.coralogix.zio.k8s.client.config.*
import com.coralogix.zio.k8s.client.config.httpclient.*
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import zio.*
import zio.http.*
import zio.http.Mode
import zio.http.codec.PathCodec.path
import zio.http.endpoint.openapi.*
import zio.http.netty.NettyConfig
import zio.logging.backend.SLF4J

object Main extends ZIOAppDefault {

  // Route ZIO logs into SLF4J so the logback appenders (STDOUT / DataDog / file) configured in
  // `src/main/resources/logback*.xml` become the single sink for the whole app — including the
  // arcane-framework's `ZIOLogAnnotations.zlog` calls, which are just ZIO logs with structured
  // annotations underneath.
  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  def m: Mode = Mode.current

  private val openAPI =
    OpenAPIGen.fromEndpoints(
      title = "Arcane Ingestion API",
      version = "2.0.0",
      DynamicServer.endpoint,
      HealthEndpoint.endpoint,
      ReadinessEndpoint.endpoint
    )
  private val swaggerRoutes = SwaggerUI.routes("docs" / "openapi", openAPI)

  def run =
    ZIO
      .scoped {
        (for {
          appConfig <- ZIO.service[AppConfig]
          _         <- zlog(s"${BuildInfo.name} ${BuildInfo.version} (${BuildInfo.gitCommit})")
          _         <- zlog(s"ZIO HTTP Mode: $m")
          _ <- zlog(
            s"Starting server on ${appConfig.server.host}:${appConfig.server.port} with ${appConfig.server.nThreads} threads"
          )
          _ <- DynamicRoutingApp.reloader
          _ <- Server.serve(
            swaggerRoutes
              ++ HealthEndpoint.routes
              ++ ReadinessEndpoint.routes
              ++ DynamicServer.routes
          )
        } yield ())
      }
      .onInterrupt(zlog("Shutting down gracefully..."))
      .provideSome[ZIOAppArgs](
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
        ReadinessSignal.live,
        RequestServiceLive.live,
        PersistenceService.live,
        IcebergProvisioner.live,
        // Observability: tag provider is always installed (metrics register in-memory even when
        // no publisher is wired); the DataDog publisher is a conditional no-op controlled by
        // `observability.datadog.enabled`.
        ObservabilityLayers.tagProviderLayer,
        IngestionMetrics.layer,
        ObservabilityLayers.publisherLayer
      )
      .tapError(err => zlog(s"Fatal startup error: ${err.getMessage}"))
}
