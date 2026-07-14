package arcane.ingestion.observability

import com.sneaksanddata.arcane.framework.services.metrics.base.MetricTagProvider
import zio.*
import zio.metrics.*

/** Ingestion-service metrics. Every metric is stamped with the tags returned by [[MetricTagProvider]] (service name,
  * static tags configured under `observability.metricTags`) so downstream collectors can slice by service/env without
  * touching individual call sites.
  *
  *   - `requestsTotal`: total ingestion HTTP requests, labeled by `producer` and `status` (`ok` / `error`).
  *   - `ingestionBytesTotal`: total bytes accepted (post-validation) per producer.
  *   - `endpointReloadTotal`: number of times the CRD-driven Routes have been rebuilt.
  *   - `activeEndpoints`: current number of dynamic ingestion endpoints served (updated on every reload).
  */
trait IngestionMetrics:
  def recordRequest(producer: String, status: String): UIO[Unit]
  def recordIngestionBytes(producer: String, bytes: Long): UIO[Unit]
  def recordEndpointReload(activeEndpointCount: Int): UIO[Unit]

object IngestionMetrics:

  def recordRequest(producer: String, status: String): URIO[IngestionMetrics, Unit] =
    ZIO.serviceWithZIO[IngestionMetrics](_.recordRequest(producer, status))

  def recordIngestionBytes(producer: String, bytes: Long): URIO[IngestionMetrics, Unit] =
    ZIO.serviceWithZIO[IngestionMetrics](_.recordIngestionBytes(producer, bytes))

  def recordEndpointReload(activeEndpointCount: Int): URIO[IngestionMetrics, Unit] =
    ZIO.serviceWithZIO[IngestionMetrics](_.recordEndpointReload(activeEndpointCount))

  private final class Live(baseTags: Set[MetricLabel]) extends IngestionMetrics:
    private val requestsCounter =
      Metric.counter("ingestion_requests_total").tagged(baseTags)

    private val bytesCounter =
      Metric.counter("ingestion_bytes_total").tagged(baseTags)

    private val reloadCounter =
      Metric.counter("endpoint_reload_total").tagged(baseTags)

    private val activeEndpointsGauge =
      Metric.gauge("active_endpoints").tagged(baseTags)

    override def recordRequest(producer: String, status: String): UIO[Unit] =
      requestsCounter
        .tagged(MetricLabel("producer", producer), MetricLabel("status", status))
        .increment

    override def recordIngestionBytes(producer: String, bytes: Long): UIO[Unit] =
      bytesCounter.tagged(MetricLabel("producer", producer)).incrementBy(bytes)

    override def recordEndpointReload(activeEndpointCount: Int): UIO[Unit] =
      reloadCounter.increment *> activeEndpointsGauge.set(activeEndpointCount.toDouble)

  val layer: ZLayer[MetricTagProvider, Nothing, IngestionMetrics] =
    ZLayer.fromZIO {
      ZIO.serviceWith[MetricTagProvider] { provider =>
        val labels = provider.getTags.iterator.map((k, v) => MetricLabel(k, v)).toSet
        new Live(labels)
      }
    }
