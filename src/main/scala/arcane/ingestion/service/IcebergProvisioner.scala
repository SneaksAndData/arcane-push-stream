package arcane.ingestion.service

import arcane.ingestion.api.v1.{IcebergColumnSpec, IcebergTableSpec}
import arcane.ingestion.config.{AppConfig, PersistenceProvider}

import com.sneaksanddata.arcane.framework.models.ddl.CreateTableRequest
import com.sneaksanddata.arcane.framework.models.schemas.MergeKeyField
import com.sneaksanddata.arcane.framework.models.settings.iceberg.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.iceberg.{IcebergCatalogFactory, IcebergSinkEntityManager}

import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.Schema
import org.apache.iceberg.rest.auth.OAuth2Properties
import org.apache.iceberg.types.{Type, Types}
import zio.*
import zio.json.ast.Json
import zio.json.DecoderOps

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
  /** Ensure the table described by `spec` exists.
    *
    * `producerId` is the route's identity in the token store: it is the value written to the DynamoDB index attribute
    * for every pushed message, and it is published on the table so arcane-stream-pull knows which partition to poll.
    */
  def provision(spec: IcebergTableSpec, producerId: String): Task[Unit]

object IcebergProvisioner:
  val live: ZLayer[AppConfig, Nothing, IcebergProvisioner] =
    ZLayer.fromFunction(new IcebergProvisionerLive(_))

  def provision(spec: IcebergTableSpec, producerId: String): RIO[IcebergProvisioner, Unit] =
    ZIO.serviceWithZIO[IcebergProvisioner](_.provision(spec, producerId))

final class IcebergProvisionerLive(config: AppConfig) extends IcebergProvisioner:
  import IcebergProvisionerLive.*

  override def provision(spec: IcebergTableSpec, producerId: String): Task[Unit] = ZIO.scoped {
    for
      settings <- ZIO.succeed(buildSettings(spec))
      factory  <- IcebergCatalogFactory.live(settings)
      manager = new IcebergSinkEntityManager(settings, factory)
      schema     <- ZIO.attempt(buildSchema(spec))
      properties <- ZIO.attempt(creationProperties(resolveColumns(spec)))
      exists     <- manager.tableExists(spec.tableName)
      _ <- ZIO.when(exists)(
        ZIO.logInfo(s"[IcebergProvisioner] table ${spec.namespace}.${spec.tableName} already exists — skipping create")
      )
      _ <- ZIO.unless(exists)(
        manager.createTable(CreateTableRequest(spec.tableName, schema, replace = false, properties = properties)) *>
          applyInitialProperties(factory, settings, spec, producerId) *>
          ZIO.logInfo(
            s"[IcebergProvisioner] created table ${spec.namespace}.${spec.tableName} " +
              s"(${resolveColumns(spec).size} columns${
                  if properties.isEmpty then "" else s", ${properties.mkString(", ")}"
                })"
          )
      )
    yield ()
  }

  /** Seed table-level properties (e.g. the stream-pull watermark COMMENT) that the CRD declared under
    * `initialProperties`, plus the watermark comment default from [[initialProperties]]. Only applied on initial
    * creation to keep the operation a one-shot bootstrap; if the table already existed we leave properties untouched.
    */
  private def applyInitialProperties(
      factory: IcebergCatalogFactory,
      settings: IcebergCatalogSettings,
      spec: IcebergTableSpec,
      producerId: String
  ): Task[Unit] =
    val properties = initialProperties(spec, producerId, config.persistence)
    for
      catalog <- factory.getCatalog
      tableId = org.apache.iceberg.catalog.TableIdentifier.of(settings.namespace, spec.tableName)
      table <- ZIO.attemptBlocking(catalog.loadTable(factory.getSessionContext, tableId))
      _ <- ZIO.attemptBlocking {
        val update = table.updateProperties()
        properties.foreach { case (k, v) => update.set(k, v) }
        update.commit()
      }
      _ <- ZIO.logInfo(
        s"[IcebergProvisioner] seeded ${properties.size} initial properties on ${spec.namespace}.${spec.tableName}"
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

  /** Table property selecting the Iceberg spec version.
    *
    * `variant` is a v3 type, so a table declaring one must be created at format version 3. It can only be set when the
    * table is created — the CRD's `initialProperties` are applied afterwards, which is too late — so it is passed
    * through [[CreateTableRequest]] instead.
    */
  val FormatVersionProperty = "format-version"

  /** Minimum spec version supporting `variant` columns. */
  val VariantFormatVersion = "3"

  /** Properties applied when the table is created. Tables carrying a variant column are pinned to format version 3;
    * tables without one keep the catalog's default, so an existing route's table is not silently upgraded.
    */
  private[service] def creationProperties(columns: Seq[IcebergColumnSpec]): Map[String, String] =
    if columns.exists(_.`type`.equalsIgnoreCase("variant")) then Map(FormatVersionProperty -> VariantFormatVersion)
    else Map.empty

  /** Table property holding the arcane-stream-pull watermark. */
  val CommentProperty = "comment"

  /** Watermark seeded as the table COMMENT when the route declares none.
    *
    * arcane-stream-pull parses the *entire* comment as its watermark JSON, and dies with "Invalid watermark value" when
    * it is absent or not JSON, so a table provisioned without one cannot be consumed until an operator issues a COMMENT
    * ON by hand. The epoch makes the consumer replay from the beginning, which is the right starting point for a table
    * that has never been read. Note the value carries no `watermark:` prefix — the comment is the JSON.
    */
  val EpochWatermarkComment = """{"timestamp":"1970-01-01T00:00:00Z"}"""

  /** Properties seeded on the table right after creation: whatever the route declared, with the watermark comment
    * filled in when it left one out, plus the plugin properties from [[pluginProperties]]. A comment the route does
    * declare is honoured as-is, so a route that wants to start from a later point keeps control of it.
    *
    * Route-declared properties are written verbatim — they belong to the table, not to this plugin — and so is
    * `comment`, which arcane-stream-pull reads under its Iceberg-standard name.
    */
  private[service] def initialProperties(
      spec: IcebergTableSpec,
      producerId: String,
      persistence: PersistenceProvider
  ): Map[String, String] =
    val declared =
      if spec.initialProperties.contains(CommentProperty) then spec.initialProperties
      else spec.initialProperties + (CommentProperty -> EpochWatermarkComment)

    declared ++ pluginProperties(spec, producerId, persistence)

  /** Everything arcane-stream-pull needs to find and decode this table's source records, published under
    * [[PushStreamPropertyPrefix]] so it cannot collide with Iceberg's own properties or with anything the route
    * declares itself.
    *
    * The token store coordinates are taken from this service's own persistence configuration, which is what actually
    * wrote the records, so a table can never advertise a store it is not fed from. An in-memory backend has no
    * coordinates to publish (it is dev-only and nothing can pull from it), so only the pointer is written.
    */
  private[service] def pluginProperties(
      spec: IcebergTableSpec,
      producerId: String,
      persistence: PersistenceProvider
  ): Map[String, String] =
    val pointer = spec.jsonExpressionPointer.filter(_.trim.nonEmpty).map(JsonPointerProperty -> _).toMap

    val tokenStore = persistence match
      case dynamo: PersistenceProvider.DynamoDB =>
        Map(
          PullIndexKeyProperty      -> dynamo.pullIndexKey,
          PullIndexValueProperty    -> producerId,
          VersionFieldNameProperty  -> dynamo.versionFieldName,
          DynamoDbRegionProperty    -> dynamo.region,
          DynamoDbTableNameProperty -> dynamo.tableName
        ) ++ dynamo.endpoint.filter(_.trim.nonEmpty).map(DynamoDbEndpointProperty -> _)
      case _: PersistenceProvider.InMemory => Map.empty

    tokenStore ++ pointer

  /** Namespace for every table property this service owns. Properties the route declares (and `comment`, which is
    * Iceberg's own) are left unprefixed; only the ones arcane-stream-pull reads back from us carry it.
    */
  val PushStreamPropertyPrefix = "arcane.plugin.push-stream."

  /** Table property carrying the route's `jsonExpressionPointer` to the consumer.
    *
    * arcane-stream-pull has to apply the same pointer this table's columns were derived from, and the PullStream CRD
    * has no field to configure it with. Publishing it on the table itself keeps the two in step by construction: the
    * table that defines the columns also states how to reach the document they describe.
    *
    * Like every entry in [[initialProperties]] it is written at creation time only, so changing a route's pointer on an
    * existing table needs the property to be updated by hand.
    */
  val JsonPointerProperty = PushStreamPropertyPrefix + "json-pointer-expression"

  /** Token-store attribute partitioning records by producer, and the value identifying this route's partition within it
    * — together they are the `pullIndexKey = pullIndexValue` predicate the consumer polls with.
    */
  val PullIndexKeyProperty   = PushStreamPropertyPrefix + "pull-index-key"
  val PullIndexValueProperty = PushStreamPropertyPrefix + "pull-index-value"

  /** Token-store attribute holding the ingestion timestamp the consumer advances its watermark over. */
  val VersionFieldNameProperty = PushStreamPropertyPrefix + "version-field-name"

  /** Coordinates of the DynamoDB table holding the tokens for this route. The endpoint is only published when one is
    * configured (dynamodb-local); against real AWS the region alone resolves it.
    */
  val DynamoDbRegionProperty    = PushStreamPropertyPrefix + "dynamodb-region"
  val DynamoDbTableNameProperty = PushStreamPropertyPrefix + "dynamodb-table-name"
  val DynamoDbEndpointProperty  = PushStreamPropertyPrefix + "dynamodb-endpoint"

  /** Column receiving the payload's own `id`, used only by routes without a pointer. It is renamed so it cannot be
    * confused with the envelope `id`, which identifies the pushed message and lands in [[MergeKeyColumn]] instead.
    *
    * The same rename is applied to the persisted document by [[renameReservedRootFields]], so the stored data and the
    * provisioned column always carry the same name.
    *
    * A pointer-bound route keeps the payload's own `id`: applying the pointer drops the envelope, so there is nothing
    * left to collide with.
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
  /** The final column list for a route: derived from the Avro `payloadSchema` when it declares one, falling back to the
    * hand-written list otherwise, with the envelope columns appended.
    */
  private[service] def resolveColumns(spec: IcebergTableSpec): Seq[IcebergColumnSpec] =
    withEnvelopeColumns(
      spec.payloadSchema.map(deriveColumns(_, spec.jsonExpressionPointer)).getOrElse(spec.columns)
    )

  def buildSchema(spec: IcebergTableSpec): Schema =
    val nestedFields = resolveColumns(spec).zipWithIndex.map { case (col, idx) =>
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

  /** Maps an Avro record schema onto the target table's columns, one column per field.
    *
    * Which record that is depends on `pointer`: without one the payload schema's own root is used, so the request body
    * maps to the table field for field. With one, the record the pointer selects is used instead and *its* fields are
    * hoisted into columns — the surrounding envelope contributes nothing. The consumer applies the same pointer, read
    * back from [[JsonPointerProperty]], so the record resolved here describes exactly the document it decodes.
    *
    * Scalar members keep their own typed column. A `record`, `map` or `array` member becomes a single `variant` column
    * instead: its shape is not known until a message arrives, so there is no set of typed columns to derive, and
    * Iceberg's variant encoding preserves the document as the producer sent it while staying queryable. This matches
    * the framework's own mapping, where those Avro types become `ObjectType` and therefore `VariantType`. Hoisting
    * therefore only ever descends one level: whatever sits below the pointed-at record stays inside a variant.
    *
    * A table holding a variant column must be created with `format-version=3`; see [[FormatVersionProperty]].
    */
  private[service] def deriveColumns(
      payloadSchema: String,
      pointer: Option[String] = None
  ): Seq[IcebergColumnSpec] =
    val record = resolvePayloadRecord(payloadSchema, pointer)
    // only a pointer-less route reaches its payload through the envelope, so only it can collide over `id`
    val renamed = JsonPointer.segments(pointer).isEmpty

    val columns = record.getFields.asScala.toSeq.map { field =>
      val fieldSchema = unwrapNullable(field.schema())
      val name        = if renamed then renameRootField(field.name()) else field.name()
      IcebergColumnSpec(name, toColumnType(fieldSchema, field.name()))
    }

    val duplicates = columns.groupBy(_.name.toLowerCase).filter(_._2.size > 1).keys
    if duplicates.nonEmpty then
      throw new IllegalArgumentException(
        s"payloadSchema declares duplicate iceberg columns: ${duplicates.mkString(", ")}. " +
          "Rename the colliding fields, since iceberg resolves columns case-insensitively."
      )

    columns

  /** Walks `pointer` through the payload schema and returns the record whose fields become the table's columns.
    *
    * Traversal only crosses record members. A pointer reaching into a map or an array cannot name a stable set of
    * fields — a map's keys are unknown until a message arrives, and an array index would pick one element out of many —
    * so those are rejected here rather than producing a table that silently fails to match the data.
    */
  private[service] def resolvePayloadRecord(
      payloadSchema: String,
      pointer: Option[String]
  ): org.apache.avro.Schema =
    val root = requireRecord(org.apache.avro.Schema.Parser().parse(payloadSchema), "payloadSchema")

    JsonPointer.segments(pointer).foldLeft(root) { (record, segment) =>
      val field = Option(record.getField(segment)).getOrElse(
        throw new IllegalArgumentException(
          s"jsonExpressionPointer '${pointer.getOrElse("")}' does not resolve against the payloadSchema: " +
            s"record '${record.getFullName}' declares no field '$segment'"
        )
      )
      requireRecord(unwrapNullable(field.schema()), s"field '$segment' selected by jsonExpressionPointer")
    }

  private def requireRecord(schema: org.apache.avro.Schema, what: String): org.apache.avro.Schema =
    if schema.getType == org.apache.avro.Schema.Type.RECORD then schema
    else
      throw new IllegalArgumentException(
        s"$what must be an Avro record to derive iceberg columns from, got '${schema.getType.getName}'"
      )

  /** The payload's root `id` becomes [[PushEventIdColumn]]; every other field keeps its name. */
  private[ingestion] def renameRootField(name: String): String =
    if name == "id" then PushEventIdColumn else name

  /** Applies the [[renameRootField]] rule to a document about to be persisted.
    *
    * The rename cannot be left to the consumer: arcane-stream-pull decodes each stored document against the target
    * table's own schema, and the PullStream CRD exposes no rename map, so a document keeping `id` while the table
    * declares `push_event_id` fails to decode. Renaming here is what keeps the two halves in agreement.
    *
    * Only an object's own top-level member is touched — those are exactly the members that became columns. A document
    * already carrying `push_event_id` is left alone, since renaming would collide with it.
    */
  private[ingestion] def renameReservedRootFields(document: String): String =
    document.fromJson[Json] match
      case Right(Json.Obj(fields)) if fields.exists(_._1 == "id") && !fields.exists(_._1 == PushEventIdColumn) =>
        Json.Obj(fields.map((name, value) => renameRootField(name) -> value)).toString
      case _ => document

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
      // a container's contents are unknown until a message arrives, so it is stored as a variant rather than being
      // flattened into typed columns or stringified
      case org.apache.avro.Schema.Type.MAP    => "variant"
      case org.apache.avro.Schema.Type.ARRAY  => "variant"
      case org.apache.avro.Schema.Type.RECORD => "variant"
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
    case "variant"   => Types.VariantType.get()
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported iceberg column type '$other' for column '${col.name}'. " +
          "Supported: string, int, long, double, float, boolean, binary, date, timestamp, variant."
      )
