package arcane.ingestion.observability

import arcane.ingestion.config.AppConfig
import com.sneaksanddata.arcane.framework.models.settings.observability.DefaultObservabilitySettings
import com.sneaksanddata.arcane.framework.services.metrics.GlobalMetricTagProvider
import com.sneaksanddata.arcane.framework.services.metrics.base.MetricTagProvider
import zio.*
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.{DatadogPublisherConfig, live as datadogLive}
import zio.metrics.connectors.statsd.{DatagramSocketConfig, statsdUDS}

/** Assembles the observability layer chain from `AppConfig.observability`.
  *
  * We construct `GlobalMetricTagProvider` directly (rather than via `GlobalMetricTagProvider.layer`, which requires a
  * full `PluginStreamContext`): this service isn't a stream plugin, so we synthesize the fields it needs — `streamKind`
  * from the configured service name, `streamId` from the JVM host, and `isBackfilling = false`.
  *
  * The DataDog publisher is only wired in when `observability.datadog.enabled = true`; otherwise the layer resolves to
  * a no-op so metrics still register (and can be scraped by other backends) without opening a socket.
  */
object ObservabilityLayers:

  /** Build a [[MetricTagProvider]] from AppConfig.
    *
    * `streamId` uses the JVM host name so per-pod metrics are distinguishable in a Kubernetes deployment.
    * `SecurityException` from `InetAddress.getHostName` is caught and swallowed by falling back to `"unknown"` — we
    * never want observability wiring to fail the whole app.
    */
  val tagProviderLayer: ZLayer[AppConfig, Nothing, MetricTagProvider] =
    ZLayer.fromZIO {
      for
        appConfig <- ZIO.service[AppConfig]
        hostName <- ZIO
          .attempt(java.net.InetAddress.getLocalHost.getHostName)
          .catchAll(_ => ZIO.succeed("unknown"))
      yield GlobalMetricTagProvider(
        streamKind = appConfig.observability.serviceName,
        isBackfilling = false,
        streamId = hostName,
        backfillId = None,
        observabilitySettings = DefaultObservabilitySettings(appConfig.observability.metricTags)
      )
    }

  private val metricsConfigLayer: ZLayer[AppConfig, Nothing, MetricsConfig] =
    ZLayer.fromZIO {
      ZIO.serviceWith[AppConfig] { cfg =>
        MetricsConfig(cfg.observability.datadog.publisherIntervalSeconds.seconds)
      }
    }

  private val socketConfigLayer: ZLayer[AppConfig, Nothing, DatagramSocketConfig] =
    ZLayer.fromZIO {
      ZIO.serviceWith[AppConfig](cfg => DatagramSocketConfig(cfg.observability.datadog.socketPath))
    }

  private val datadogPublisherConfigLayer: ULayer[DatadogPublisherConfig] =
    ZLayer.succeed(DatadogPublisherConfig())

  /** Publisher: DogStatsD UDS if enabled, no-op otherwise. Depends on `AppConfig` so it can read the runtime toggle.
    * Registers a listener on the ZIO metrics client for the lifetime of the app scope — using `ZLayer.scoped` here so
    * the inner chain's finalizers (socket close, listener removal) attach to the enclosing app scope, not to a
    * transient sub-scope.
    */
  val publisherLayer: ZLayer[AppConfig, Nothing, Unit] =
    ZLayer.scoped {
      ZIO.serviceWithZIO[AppConfig] { cfg =>
        if cfg.observability.datadog.enabled then
          val cfgLayer: ULayer[AppConfig] = ZLayer.succeed(cfg)
          val chain: ZLayer[Any, Nothing, Unit] =
            (cfgLayer >>> (socketConfigLayer ++ metricsConfigLayer)) ++ datadogPublisherConfigLayer >>>
              (statsdUDS ++ datadogPublisherConfigLayer ++ (cfgLayer >>> metricsConfigLayer)) >>>
              datadogLive
          ZIO.logInfo(
            s"DataDog UDS publisher enabled on ${cfg.observability.datadog.socketPath} " +
              s"(interval ${cfg.observability.datadog.publisherIntervalSeconds}s)"
          ) *> chain.build.unit
        else ZIO.logInfo("Observability: DataDog UDS publisher disabled").unit
      }
    }
