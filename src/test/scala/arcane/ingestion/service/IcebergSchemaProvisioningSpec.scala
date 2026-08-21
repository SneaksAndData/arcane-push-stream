package arcane.ingestion.service

import arcane.ingestion.api.v1.{IcebergColumnSpec, IcebergTableSpec}

import org.apache.iceberg.types.Types
import zio.*
import zio.test.*
import zio.test.Assertion.*

import scala.jdk.CollectionConverters.*

/** Covers the table layout provisioned for a route, which has to match what arcane-stream-pull writes: one column per
  * root payload field, nested containers as `variant`, plus the two envelope columns the framework supplies.
  */
object IcebergSchemaProvisioningSpec extends ZIOSpecDefault:

  /** A route whose payload members are strongly typed, so each becomes a column of its own Iceberg type. */
  private val nestedRecordSchema =
    """
      |{
      |  "type": "record",
      |  "name": "Producer2Event",
      |  "namespace": "com.sneaksanddata.pushstream",
      |  "fields": [
      |    { "name": "id",           "type": "string" },
      |    { "name": "timestampUTC", "type": "string" },
      |    {
      |      "name": "payload",
      |      "type": {
      |        "type": "record",
      |        "name": "Producer2Payload",
      |        "namespace": "com.sneaksanddata.pushstream",
      |        "fields": [
      |          { "name": "eventType",  "type": "string" },
      |          { "name": "sequence",   "type": "int" },
      |          { "name": "durationMs", "type": "long" },
      |          { "name": "score",      "type": "double" },
      |          { "name": "isRetry",    "type": "boolean" }
      |        ]
      |      }
      |    }
      |  ]
      |}
      |""".stripMargin

  /** A route whose payload is a `map`, whose keys are unknown until a message arrives. */
  private val mapPayloadSchema =
    """
      |{
      |  "type": "record",
      |  "name": "Producer1Event",
      |  "namespace": "com.sneaksanddata.pushstream",
      |  "fields": [
      |    { "name": "id",           "type": "string" },
      |    { "name": "timestampUTC", "type": "string" },
      |    { "name": "payload",      "type": { "type": "map", "values": "string" } }
      |  ]
      |}
      |""".stripMargin

  private def specOf(
      payloadSchema: Option[String],
      columns: Seq[IcebergColumnSpec] = Seq.empty,
      initialProperties: Map[String, String] = Map.empty
  ) =
    IcebergTableSpec(
      catalogUri = "http://localhost:20001/catalog",
      warehouse = "lakehouse-bronze",
      namespace = "arcane_pull_test",
      tableName = "events",
      columns = columns,
      payloadSchema = payloadSchema,
      initialProperties = initialProperties
    )

  private def layoutOf(spec: IcebergTableSpec): Seq[(String, String)] =
    IcebergProvisionerLive
      .buildSchema(spec)
      .columns()
      .asScala
      .toSeq
      .map(column => column.name() -> column.`type`().toString)

  def spec = suite("IcebergProvisionerLive.buildSchema")(
    test("stores a nested payload record in a single variant column") {
      val layout = layoutOf(specOf(Some(nestedRecordSchema)))

      // the members are not hoisted into typed columns: their shape is only known once a message arrives, and
      // variant keeps the document queryable without pinning the table to today's payload
      assertTrue(
        layout == Seq(
          "push_event_id" -> "string",
          "timestampUTC"  -> "string",
          "payload"       -> "variant",
          // the payload declares no merge key, so the framework's canonical column is appended
          "arcane_merge_key" -> "string"
        )
      )
    },
    test("stores a map payload in a variant column, since its keys are unknown when the table is created") {
      val layout = layoutOf(specOf(Some(mapPayloadSchema)))

      assertTrue(
        layout == Seq(
          "push_event_id"    -> "string",
          "timestampUTC"     -> "string",
          "payload"          -> "variant",
          "arcane_merge_key" -> "string"
        )
      )
    },
    test("stores an array payload in a variant column") {
      val arraySchema =
        """{"type":"record","name":"E","fields":[{"name":"lines","type":{"type":"array","items":"string"}}]}"""

      assertTrue(layoutOf(specOf(Some(arraySchema))).contains("lines" -> "variant"))
    },
    test("pins a table holding a variant column to format version 3") {
      // variant is a v3 type, and the property can only be set at creation, so it travels with the create request
      assertTrue(
        IcebergProvisionerLive.creationProperties(
          IcebergProvisionerLive.resolveColumns(specOf(Some(nestedRecordSchema)))
        ) == Map("format-version" -> "3")
      )
    },
    test("leaves the format version alone for a table without a variant column") {
      // an existing route must not be silently upgraded to v3 when its table is provisioned
      assertTrue(
        IcebergProvisionerLive
          .creationProperties(
            IcebergProvisionerLive.resolveColumns(specOf(None, columns = Seq(IcebergColumnSpec("id", "string"))))
          )
          .isEmpty
      )
    },
    test("adds both envelope columns to a route that declares columns by hand") {
      val layout = layoutOf(
        specOf(None, columns = Seq(IcebergColumnSpec("id", "string"), IcebergColumnSpec("payload", "string")))
      )

      // without these the stream-pull merge fails to resolve arcane_merge_key and silently drops the watermark
      assertTrue(
        layout == Seq(
          "id"               -> "string",
          "payload"          -> "string",
          "timestampUTC"     -> "string",
          "arcane_merge_key" -> "string"
        )
      )
    },
    test("does not duplicate an envelope column the route already declares, whatever its casing") {
      val layout = layoutOf(
        specOf(
          None,
          columns = Seq(
            IcebergColumnSpec("id", "string"),
            IcebergColumnSpec("timestamputc", "string"),
            IcebergColumnSpec("ARCANE_MERGE_KEY", "string")
          )
        )
      )

      assertTrue(layout == Seq("id" -> "string", "timestamputc" -> "string", "ARCANE_MERGE_KEY" -> "string"))
    },
    test("assigns field ids in declaration order starting at 1") {
      val ids = IcebergProvisionerLive.buildSchema(specOf(Some(nestedRecordSchema))).columns().asScala.map(_.fieldId())

      assertTrue(ids.toSeq == (1 to 4).toSeq)
    },
    test("makes every column optional, as the framework requires") {
      val required = IcebergProvisionerLive
        .buildSchema(specOf(Some(nestedRecordSchema)))
        .columns()
        .asScala
        .filter(_.isRequired)

      assertTrue(required.isEmpty)
    },
    test("prefers the payload schema over a stale hand-written column list") {
      val layout = layoutOf(
        specOf(Some(nestedRecordSchema), columns = Seq(IcebergColumnSpec("legacy_column", "string")))
      )

      assertTrue(!layout.map(_._1).contains("legacy_column"))
    },
    test("unwraps a nullable union to the underlying column type") {
      val schema =
        """{"type":"record","name":"E","fields":[{"name":"count","type":["null","int"],"default":null}]}"""

      assertTrue(layoutOf(specOf(Some(schema))).contains("count" -> "int"))
    },
    test("rejects a payload schema whose columns differ only by case") {
      val colliding =
        """
          |{
          |  "type": "record",
          |  "name": "E",
          |  "fields": [
          |    { "name": "source", "type": "string" },
          |    { "name": "Source", "type": "string" }
          |  ]
          |}
          |""".stripMargin

      // iceberg resolves columns case-insensitively, so one would shadow the other
      assertZIO(ZIO.attempt(IcebergProvisionerLive.buildSchema(specOf(Some(colliding)))).exit)(
        fails(isSubtype[IllegalArgumentException](hasMessage(containsString("source"))))
      )
    },
    test("no longer treats a nested field as colliding with a root one") {
      val nestedSameName =
        """
          |{
          |  "type": "record",
          |  "name": "E",
          |  "fields": [
          |    { "name": "source", "type": "string" },
          |    { "name": "payload", "type": {
          |        "type": "record", "name": "P",
          |        "fields": [ { "name": "source", "type": "string" } ]
          |    } }
          |  ]
          |}
          |""".stripMargin

      // the nested member stays inside the variant document, so it cannot shadow the root column any more
      assertTrue(
        layoutOf(specOf(Some(nestedSameName))).contains("source"  -> "string"),
        layoutOf(specOf(Some(nestedSameName))).contains("payload" -> "variant")
      )
    },
    test("rejects a payload schema that is not a record") {
      assertZIO(
        ZIO.attempt(IcebergProvisionerLive.buildSchema(specOf(Some("""{"type":"map","values":"string"}""")))).exit
      )(
        fails(isSubtype[IllegalArgumentException](hasMessage(containsString("must be an Avro record"))))
      )
    },
    test("seeds an epoch watermark comment on a route that declares none") {
      // arcane-stream-pull parses the whole comment as its watermark and refuses to start without one, so a table
      // provisioned without a comment would need a manual COMMENT ON before it could ever be consumed
      assertTrue(
        IcebergProvisionerLive.initialProperties(specOf(Some(nestedRecordSchema))) ==
          Map("comment" -> """{"timestamp":"1970-01-01T00:00:00Z"}""")
      )
    },
    test("keeps a watermark comment the route declares itself") {
      val declared = """{"timestamp":"2026-01-01T00:00:00Z"}"""
      assertTrue(
        IcebergProvisionerLive.initialProperties(
          specOf(Some(nestedRecordSchema), initialProperties = Map("comment" -> declared))
        ) == Map("comment" -> declared)
      )
    },
    test("seeds the watermark comment alongside other declared properties") {
      assertTrue(
        IcebergProvisionerLive.initialProperties(
          specOf(Some(nestedRecordSchema), initialProperties = Map("owner" -> "data-platform"))
        ) == Map("owner" -> "data-platform", "comment" -> """{"timestamp":"1970-01-01T00:00:00Z"}""")
      )
    }
  )
