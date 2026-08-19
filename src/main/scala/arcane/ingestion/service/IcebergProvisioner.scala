package arcane.ingestion.service

import arcane.ingestion.api.v1.{IcebergColumnSpec, IcebergTableSpec}

import com.sneaksanddata.arcane.framework.models.ddl.CreateTableRequest
import com.sneaksanddata.arcane.framework.models.schemas.MergeKeyField
import com.sneaksanddata.arcane.framework.models.settings.iceberg.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.iceberg.{IcebergCatalogFactory, IcebergSinkEntityManager}

import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.Schema
import org.apache.iceberg.rest.auth.OAuth2Properties
import org.apache.iceberg.types.{Type, Types}
import zio.*

import java.time.{Duration as JavaDuration}
import scala.jdk.CollectionConverters.*

/** Provisions Iceberg tables described by [[IcebergTableSpec]] values pulled from `DataRoute` CRDs.
  *
  * Semantics for this first iteration:
  *   - `provision` is idempotent: if the target table already exists in the referenced namespace it is left untouched
  *     and the call succeeds;
  *   - schema compatibility checks are deferred. A follow-up will compare the CRD schema against the catalog schema and
  *     either evolve the table (if additive) or fail loudly / mint a new table for breaking changes;
  *   - the catalog REST endpoint is contacted lazily, on every call. There is no global, long-lived catalog instance to
  *     keep tests/start-up simple. Plain HTTP catalogs (lakekeeper with `CATALOG_NO_AUTH=1`) work out of the box;
  *     OAuth-secured catalogs use `ARCANE_FRAMEWORK__S3_CATALOG_AUTH_*` env vars (unless
  *     `ARCANE_FRAMEWORK__CATALOG_NO_AUTH` is present).
  */
trait IcebergProvisioner:
  def provision(spec: IcebergTableSpec): Task[Unit]

object IcebergProvisioner:
  val live: ULayer[IcebergProvisioner] = ZLayer.succeed(new IcebergProvisionerLive)

  def provision(spec: IcebergTableSpec): RIO[IcebergProvisioner, Unit] =
    ZIO.serviceWithZIO[IcebergProvisioner](_.provision(spec))

final class IcebergProvisionerLive extends IcebergProvisioner:
  import IcebergProvisionerLive.*

  override def provision(spec: IcebergTableSpec): Task[Unit] = ZIO.scoped {
    for
      settings <- ZIO.succeed(buildSettings(spec))
      factory  <- IcebergCatalogFactory.live(settings)
      manager = new IcebergSinkEntityManager(settings, factory)
      schema <- ZIO.attempt(buildSchema(spec))
      exists <- manager.tableExists(spec.tableName)
      _ <- ZIO.when(exists)(
        ZIO.logInfo(s"[IcebergProvisioner] table ${spec.namespace}.${spec.tableName} already exists — skipping create")
      )
      _ <- ZIO.unless(exists)(
        manager.createTable(CreateTableRequest(spec.tableName, schema, replace = false)) *>
          applyInitialProperties(factory, settings, spec) *>
          ZIO.logInfo(
            s"[IcebergProvisioner] created table ${spec.namespace}.${spec.tableName} (${spec.columns.size} columns)"
          )
      )
    yield ()
  }

  /** Seed table-level properties (e.g. the stream-pull watermark COMMENT) that the CRD declared under
    * `initialProperties`. Only applied on initial creation to keep the operation a one-shot bootstrap; if the table
    * already existed we leave properties untouched.
    */
  private def applyInitialProperties(
      factory: IcebergCatalogFactory,
      settings: IcebergCatalogSettings,
      spec: IcebergTableSpec
  ): Task[Unit] =
    if spec.initialProperties.isEmpty then ZIO.unit
    else
      for
        catalog <- factory.getCatalog
        tableId = org.apache.iceberg.catalog.TableIdentifier.of(settings.namespace, spec.tableName)
        table <- ZIO.attemptBlocking(catalog.loadTable(factory.getSessionContext, tableId))
        _ <- ZIO.attemptBlocking {
          val update = table.updateProperties()
          spec.initialProperties.foreach { case (k, v) => update.set(k, v) }
          update.commit()
        }
        _ <- ZIO.logInfo(
          s"[IcebergProvisioner] seeded ${spec.initialProperties.size} initial properties on ${spec.namespace}.${spec.tableName}"
        )
      yield ()

object IcebergProvisionerLive:
  private val CatalogNoAuthEnv            = "ARCANE_FRAMEWORK__CATALOG_NO_AUTH"
  private val AuthClientIdEnv             = "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_ID"
  private val AuthClientSecretEnv         = "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_SECRET"
  private val AuthClientUriEnv            = "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI"
  private val AuthScopeEnv                = "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SCOPE"
  private val AuthStaticTokenEnv          = "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_STATIC_TOKEN"
  private val AuthTokenRefreshEnabledEnv  = "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_TOKEN_REFRESH_ENABLED"
  private val AuthSessionTimeoutMillisEnv = "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SESSION_TIMEOUT_MILLIS"
  private val DefaultSessionTimeoutMillis = JavaDuration.ofMinutes(55).toMillis.toString

  /** Build in-memory [[IcebergCatalogSettings]] from a CRD spec.
    *
    * For no-auth catalogs, set `ARCANE_FRAMEWORK__CATALOG_NO_AUTH` to skip auth properties. Otherwise OAuth2 properties
    * are derived from `ARCANE_FRAMEWORK__S3_CATALOG_AUTH_*`.
    */
  def buildSettings(spec: IcebergTableSpec): IcebergCatalogSettings = new IcebergCatalogSettings:
    override val namespace: String                         = spec.namespace
    override val warehouse: String                         = spec.warehouse
    override val catalogUri: String                        = spec.catalogUri
    override val additionalProperties: Map[String, String] = buildAdditionalPropertiesFromEnv(sys.env)
    override val maxCatalogInstanceLifetime: zio.Duration  = 1.hour

  private[service] def buildAdditionalPropertiesFromEnv(env: Map[String, String]): Map[String, String] =
    if isNoAuthEnabled(env) then Map.empty
    else
      val oauth2Uri   = requiredEnv(env, AuthClientUriEnv)
      val oauth2Scope = requiredEnv(env, AuthScopeEnv)
      val authBaseProperties =
        env.get(AuthStaticTokenEnv).filter(_.nonEmpty) match
          case Some(staticToken) =>
            Map(
              OAuth2Properties.TOKEN             -> staticToken,
              OAuth2Properties.OAUTH2_SERVER_URI -> oauth2Uri,
              OAuth2Properties.SCOPE             -> oauth2Scope
            )
          case None =>
            val clientId     = requiredEnv(env, AuthClientIdEnv)
            val clientSecret = requiredEnv(env, AuthClientSecretEnv)
            Map(
              OAuth2Properties.CREDENTIAL        -> s"$clientId:$clientSecret",
              OAuth2Properties.OAUTH2_SERVER_URI -> oauth2Uri,
              OAuth2Properties.SCOPE             -> oauth2Scope
            )

      authBaseProperties ++ Map(
        OAuth2Properties.TOKEN_REFRESH_ENABLED -> env.getOrElse(AuthTokenRefreshEnabledEnv, "false").toLowerCase,
        CatalogProperties.AUTH_SESSION_TIMEOUT_MS -> env.getOrElse(
          AuthSessionTimeoutMillisEnv,
          DefaultSessionTimeoutMillis
        )
      )

  private def requiredEnv(env: Map[String, String], key: String): String =
    env.get(key).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(s"Environment variable '$key' must be set for Iceberg catalog authentication")
    }

  private def isNoAuthEnabled(env: Map[String, String]): Boolean =
    env.get(CatalogNoAuthEnv).exists(v => Set("1", "true", "yes", "on").contains(v.trim.toLowerCase))

  /** Column receiving the payload's own `id`. It is renamed so it cannot be confused with the envelope `id`, which
    * identifies the pushed message and lands in [[MergeKeyColumn]] instead.
    *
    * Must agree with the `jsonArrayPointers` rename map configured on the consuming arcane-stream-pull instance.
    */
  val PushEventIdColumn = "push_event_id"

  /** Ingestion timestamp. Written by [[PersistenceService]] as its own DynamoDB attribute rather than being part of the
    * pushed body, and appended to every row by the stream-pull source; the target table must declare it or the value is
    * silently dropped.
    */
  val TimestampColumn = "timestampUTC"

  /** Row identity required by the framework's MERGE. The schema conversion appends this field to every staged batch
    * whether or not the target declares it, so a target without the column fails the merge with
    * `Column 't_o.arcane_merge_key' cannot be resolved`.
    */
  val MergeKeyColumn: String = MergeKeyField.name.toLowerCase

  /** Build an Iceberg [[Schema]] for the target table, assigning monotonic field-ids starting at 1 in declaration
    * order. This is the same scheme the framework's [[org.apache.iceberg.Schema]] uses when re-loading by column index,
    * so reordering columns of an existing table is forbidden — guard against that in a future schema-evolution pass.
    *
    * Columns come from the route's Avro `payloadSchema` when it declares one, falling back to the hand-written column
    * list otherwise. Either way the two envelope columns the framework supplies are appended, so a table provisioned
    * here is directly mergeable by arcane-stream-pull.
    *
    * It is important to keep the schema fields NOT required, because framework expects all fields to be NULLABLE.
    */
  def buildSchema(spec: IcebergTableSpec): Schema =
    val declared = spec.payloadSchema.map(deriveColumns).getOrElse(spec.columns)
    val nestedFields = withEnvelopeColumns(declared).zipWithIndex.map { case (col, idx) =>
      val id = idx + 1
      val t  = toIcebergType(col)
      Types.NestedField.optional(id, col.name, t)
    }
    new Schema(nestedFields.asJava)

  /** Appends the columns the stream-pull source adds to every row, unless the route already declares them. The match is
    * case-insensitive because the source resolves both attributes that way.
    */
  private[service] def withEnvelopeColumns(columns: Seq[IcebergColumnSpec]): Seq[IcebergColumnSpec] =
    val declaredNames = columns.map(_.name.toLowerCase).toSet
    val envelope = Seq(TimestampColumn, MergeKeyColumn)
      .filterNot(name => declaredNames.contains(name.toLowerCase))
      .map(name => IcebergColumnSpec(name, "string"))

    columns ++ envelope

  /** Flattens an Avro record schema into the target table's columns.
    *
    * The stream-pull source hoists the members of a nested record up to the root, so the table declares one column per
    * nested member rather than a single JSON blob. A `map` or `array` member cannot be flattened — its keys are not
    * known until a message arrives — so it keeps one string column, which is what the decoder writes for a container
    * aimed at a string field.
    */
  private[service] def deriveColumns(payloadSchema: String): Seq[IcebergColumnSpec] =
    val parsed = org.apache.avro.Schema.Parser().parse(payloadSchema)

    if parsed.getType != org.apache.avro.Schema.Type.RECORD then
      throw new IllegalArgumentException(
        s"payloadSchema must be an Avro record to derive iceberg columns from, got '${parsed.getType.getName}'"
      )

    val columns = parsed.getFields.asScala.toSeq.flatMap { field =>
      val fieldSchema = unwrapNullable(field.schema())
      fieldSchema.getType match
        case org.apache.avro.Schema.Type.RECORD =>
          fieldSchema.getFields.asScala.toSeq.map(nested =>
            IcebergColumnSpec(nested.name(), toColumnType(unwrapNullable(nested.schema()), nested.name()))
          )
        case _ =>
          Seq(IcebergColumnSpec(renameRootField(field.name()), toColumnType(fieldSchema, field.name())))
    }

    val duplicates = columns.groupBy(_.name.toLowerCase).filter(_._2.size > 1).keys
    if duplicates.nonEmpty then
      throw new IllegalArgumentException(
        s"payloadSchema flattens to duplicate iceberg columns: ${duplicates.mkString(", ")}. " +
          "Rename the colliding fields, since a hoisted field would otherwise shadow a root one."
      )

    columns

  /** The payload's root `id` becomes [[PushEventIdColumn]]; every other field keeps its name. */
  private def renameRootField(name: String): String =
    if name == "id" then PushEventIdColumn else name

  /** Optional Avro fields are encoded as `["null", T]`; the target column type is `T`. */
  private def unwrapNullable(schema: org.apache.avro.Schema): org.apache.avro.Schema =
    if schema.getType == org.apache.avro.Schema.Type.UNION then
      schema.getTypes.asScala.filter(_.getType != org.apache.avro.Schema.Type.NULL).toSeq match
        case single :: Nil => single
        case _             => schema
    else schema

  private def toColumnType(schema: org.apache.avro.Schema, fieldName: String): String =
    schema.getType match
      case org.apache.avro.Schema.Type.STRING  => "string"
      case org.apache.avro.Schema.Type.ENUM    => "string"
      case org.apache.avro.Schema.Type.INT     => "int"
      case org.apache.avro.Schema.Type.LONG    => "long"
      case org.apache.avro.Schema.Type.FLOAT   => "float"
      case org.apache.avro.Schema.Type.DOUBLE  => "double"
      case org.apache.avro.Schema.Type.BOOLEAN => "boolean"
      case org.apache.avro.Schema.Type.BYTES   => "binary"
      // keys are unknown until a message arrives, so the container is stored as the json text the decoder produces
      case org.apache.avro.Schema.Type.MAP    => "string"
      case org.apache.avro.Schema.Type.ARRAY  => "string"
      case org.apache.avro.Schema.Type.RECORD => "string"
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported avro type '${other.getName}' for payloadSchema field '$fieldName'"
        )

  private def toIcebergType(col: IcebergColumnSpec): Type = col.`type`.toLowerCase match
    case "string"    => Types.StringType.get()
    case "int"       => Types.IntegerType.get()
    case "long"      => Types.LongType.get()
    case "double"    => Types.DoubleType.get()
    case "float"     => Types.FloatType.get()
    case "boolean"   => Types.BooleanType.get()
    case "binary"    => Types.BinaryType.get()
    case "date"      => Types.DateType.get()
    case "timestamp" => Types.TimestampType.withZone()
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported iceberg column type '$other' for column '${col.name}'. " +
          "Supported: string, int, long, double, float, boolean, binary, date, timestamp."
      )
