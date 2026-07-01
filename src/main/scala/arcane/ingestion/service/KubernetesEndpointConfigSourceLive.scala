/*Kubernetes CRD watch service
 *
 * The idea is to build a map for the list of producers from the CRDs:
 * and use a `watchForever` stream to check all sorts of changes.
 *
 * */
package arcane.ingestion.api.v1

import zio.*
import zio.stream.{SubscriptionRef, ZStream}

import com.coralogix.zio.k8s.client.com.sneaksanddata.ingestion.v1alpha1.dataroutes
import com.coralogix.zio.k8s.client.com.sneaksanddata.ingestion.v1alpha1.dataroutes.DataRoutes
import com.coralogix.zio.k8s.client.com.sneaksanddata.ingestion.definitions.dataroute.v1alpha1.DataRoute
import com.coralogix.zio.k8s.client.model.{Added, Deleted, K8sNamespace, Modified, Reseted, TypedWatchEvent}

/** Kubernetes CRD-backed [[EndpointConfigSource]].
  *
  * Watches `DataRoute` custom resources in a single namespace and folds the watch event stream into a
  * `Map[name → EndpointConfig]`. Each snapshot is published to a [[SubscriptionRef]] so subscribers (the
  * [[DynamicRoutingApp.reloader]] fiber) react to every change with at most one Routes rebuild.
  *
  * The underlying `watchForever` transparently re-establishes the watch on disconnect and emits a
  * `TypedWatchEvent.Reseted()` after a full re-sync (e.g. after `410 Gone`), which we use to reset the local snapshot
  * map.
  *
  * @param namespace
  *   the namespace to watch. Use `None` for cluster-wide.
  */
final class KubernetesEndpointConfigSourceLive private (
    snapshots: SubscriptionRef[List[EndpointConfig]]
) extends EndpointConfigSource:
  def watch: ZStream[Any, Nothing, List[EndpointConfig]] = snapshots.changes

object KubernetesEndpointConfigSourceLive:

  /** Build the layer.
    *
    * @param namespace
    *   the K8s namespace to watch for `DataRoute` resources. Pass `None` to watch across all namespaces (requires
    *   cluster-wide RBAC).
    */
  def layer(namespace: Option[K8sNamespace]): ZLayer[DataRoutes, Nothing, EndpointConfigSource] =
    ZLayer.scoped {
      for
        ref <- SubscriptionRef.make(List.empty[EndpointConfig])
        _   <- runWatch(namespace, ref).forkScoped
      yield new KubernetesEndpointConfigSourceLive(ref)
    }

  /** Continuously fold the watch event stream into a snapshot and push to the ref.
    *
    * Recovers from any `K8sFailure` by logging and retrying with exponential backoff — we never want the watch fiber to
    * die silently and leave the route table stale.
    */
  private def runWatch(
      namespace: Option[K8sNamespace],
      ref: SubscriptionRef[List[EndpointConfig]]
  ): ZIO[DataRoutes, Nothing, Unit] =
    dataroutes
      .watchForever(namespace = namespace)
      .scan(Map.empty[String, EndpointConfig]) {
        case (acc, Added(cr: DataRoute))    => upsert(acc, cr)
        case (acc, Modified(cr: DataRoute)) => upsert(acc, cr)
        case (acc, Deleted(cr: DataRoute))  => remove(acc, cr)
        case (_, Reseted())                 => Map.empty
      }
      .map(_.values.toList.sortBy(_.producerId))
      .changes
      .foreach(ref.set)
      .tapError(e => ZIO.logWarningCause(s"DataRoute watch error, will retry: $e", Cause.fail(e)))
      .retry(Schedule.exponential(200.millis).jittered)
      .ignore

  private def upsert(
      acc: Map[String, EndpointConfig],
      cr: DataRoute
  ): Map[String, EndpointConfig] =
    (for
      meta <- cr.metadata.toOption
      name <- meta.name.toOption
      spec <- cr.spec.toOption
    yield acc + (name -> EndpointConfig(
      producerId = spec.consumerId,
      schemaSubject = spec.schemaSubject,
      schemaVersion = spec.schemaVersion.toInt,
      payloadSchema = spec.payloadSchema.toOption,
      iceberg = toIcebergSpec(spec)
    ))).getOrElse(acc)

  /** Project the generated CRD `spec.iceberg` block (if present) onto our local [[IcebergTableSpec]]. Fields declared
    * `required` in the CRD schema arrive as plain values from codegen; only optional fields need `.toOption`.
    */
  private def toIcebergSpec(spec: DataRoute.Spec): Option[IcebergTableSpec] =
    spec.iceberg.toOption.flatMap { ice =>
      val cols = ice.columns.toSeq.map { c =>
        IcebergColumnSpec(
          name = c.name,
          `type` = c.`type`.value,
          required = c.required.toOption.getOrElse(false)
        )
      }
      Option.when(cols.nonEmpty)(
        IcebergTableSpec(
          catalogUri = ice.catalogUri,
          warehouse = ice.warehouse,
          namespace = ice.namespace,
          tableName = ice.tableName,
          columns = cols,
          initialProperties = ice.initialProperties.toOption
            .map(_.iterator.toMap)
            .getOrElse(Map.empty)
        )
      )
    }

  private def remove(
      acc: Map[String, EndpointConfig],
      cr: DataRoute
  ): Map[String, EndpointConfig] =
    cr.metadata.toOption.flatMap(_.name.toOption).map(acc - _).getOrElse(acc)
