package arcane.ingestion.service

import arcane.ingestion.api.v1.{IcebergColumnSpec, IcebergTableSpec}

import org.apache.iceberg.types.Types
import zio.*
import zio.test.*
import zio.test.Assertion.*

import scala.jdk.CollectionConverters.*

/** Covers the table layout provisioned for a route, which has to match what arcane-stream-pull writes: the payload
  * flattened into columns, plus the two envelope columns the framework supplies.
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

  private def specOf(payloadSchema: Option[String], columns: Seq[IcebergColumnSpec] = Seq.empty) =
    IcebergTableSpec(
      catalogUri = "http://localhost:20001/catalog",
      warehouse = "lakehouse-bronze",
      namespace = "arcane_pull_test",
      tableName = "events",
      columns = columns,
      payloadSchema = payloadSchema
    )

  private def layoutOf(spec: IcebergTableSpec): Seq[(String, String)] =
    IcebergProvisionerLive
      .buildSchema(spec)
      .columns()
      .asScala
      .toSeq
      .map(column => column.name() -> column.`type`().toString)

  def spec = suite("IcebergProvisionerLive.buildSchema")(
    test("explodes a nested payload record into one typed column per member") {
      val layout = layoutOf(specOf(Some(nestedRecordSchema)))

      assertTrue(
        layout == Seq(
          "push_event_id" -> "string",
          "timestampUTC"  -> "string",
          "eventType"     -> "string",
          "sequence"      -> "int",
          "durationMs"    -> "long",
          "score"         -> "double",
          "isRetry"       -> "boolean",
          // the payload declares no merge key, so the framework's canonical column is appended
          "arcane_merge_key" -> "string"
        )
      )
    },
    test("keeps a map payload in a single string column, since its keys are unknown when the table is created") {
      val layout = layoutOf(specOf(Some(mapPayloadSchema)))

      assertTrue(
        layout == Seq(
          "push_event_id"    -> "string",
          "timestampUTC"     -> "string",
          "payload"          -> "string",
          "arcane_merge_key" -> "string"
        )
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

      assertTrue(ids.toSeq == (1 to 8).toSeq)
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
    test("rejects a payload schema that flattens to colliding column names") {
      val colliding =
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

      // hoisting would silently drop one of them, so provisioning refuses the route instead
      assertZIO(ZIO.attempt(IcebergProvisionerLive.buildSchema(specOf(Some(colliding)))).exit)(
        fails(isSubtype[IllegalArgumentException](hasMessage(containsString("source"))))
      )
    },
    test("rejects a payload schema that is not a record") {
      assertZIO(
        ZIO.attempt(IcebergProvisionerLive.buildSchema(specOf(Some("""{"type":"map","values":"string"}""")))).exit
      )(
        fails(isSubtype[IllegalArgumentException](hasMessage(containsString("must be an Avro record"))))
      )
    }
  )
