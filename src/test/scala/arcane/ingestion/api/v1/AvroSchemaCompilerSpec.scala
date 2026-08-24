package arcane.ingestion.api.v1

import zio.test.*

/** Covers the plain-JSON ingestion contract: producers POST documents as any JSON serializer would write them, and the
  * union branch tags Avro's JSON decoder requires are inferred rather than demanded from the caller.
  *
  * The guarantee being pinned down is that alignment only ever *adds* tags. Every document that used to be accepted
  * still is, and no document that should be rejected slips through because a tag was guessed for it.
  */
object AvroSchemaCompilerSpec extends ZIOSpecDefault:

  private def compile(schema: String) = AvroSchemaCompiler.compile(schema).toOption.get

  private val optionalScalars = compile(
    """{
      |  "type": "record", "name": "Reading", "namespace": "com.sneaksanddata.pushstream",
      |  "fields": [
      |    { "name": "MachineName", "type": "string" },
      |    { "name": "ShoeSize", "type": "long" },
      |    { "name": "MixingGapS1", "type": ["null", "double"], "default": null },
      |    { "name": "WeightPercentageS1", "type": ["null", "long"], "default": null },
      |    { "name": "ShiftName", "type": ["null", "string"], "default": null }
      |  ]
      |}""".stripMargin
  )

  def spec = suite("CompiledAvroSchema.validateAndEncode")(
    test("accepts plain JSON, inferring the union branch of every optional field") {
      val body =
        """{"MachineName":"PT40-02","ShoeSize":42,"MixingGapS1":0.35,"WeightPercentageS1":4,"ShiftName":"Day"}"""

      assertTrue(optionalScalars.validateAndEncode(body).isRight)
    },
    test("still accepts the tagged encoding, so producers written against the old contract keep working") {
      val body =
        """{"MachineName":"PT40-02","ShoeSize":42,"MixingGapS1":{"double":0.35},
          |"WeightPercentageS1":{"long":4},"ShiftName":{"string":"Day"}}""".stripMargin

      assertTrue(optionalScalars.validateAndEncode(body).isRight)
    },
    test("encodes plain and tagged forms of the same document to identical bytes") {
      val plain = """{"MachineName":"m","ShoeSize":1,"MixingGapS1":0.5,"WeightPercentageS1":4,"ShiftName":"Day"}"""
      val tagged = """{"MachineName":"m","ShoeSize":1,"MixingGapS1":{"double":0.5},
                     |"WeightPercentageS1":{"long":4},"ShiftName":{"string":"Day"}}""".stripMargin

      val encodedPlain  = optionalScalars.validateAndEncode(plain).toOption.get
      val encodedTagged = optionalScalars.validateAndEncode(tagged).toOption.get

      assertTrue(encodedPlain.sameElements(encodedTagged))
    },
    test("reads an explicit null into the null branch") {
      val body = """{"MachineName":"m","ShoeSize":1,"MixingGapS1":null,"WeightPercentageS1":null,"ShiftName":null}"""

      assertTrue(optionalScalars.validateAndEncode(body).isRight)
    },
    test("reads a whole number into a double branch, as JSON does not distinguish 4 from 4.0") {
      val body = """{"MachineName":"m","ShoeSize":1,"MixingGapS1":4,"WeightPercentageS1":4,"ShiftName":"Day"}"""

      assertTrue(optionalScalars.validateAndEncode(body).isRight)
    },
    test("rejects a fractional value for a long branch rather than silently truncating it") {
      val body = """{"MachineName":"m","ShoeSize":1,"MixingGapS1":0.5,"WeightPercentageS1":4.5,"ShiftName":"Day"}"""

      assertTrue(optionalScalars.validateAndEncode(body).isLeft)
    },
    test("rejects a value whose type matches no branch of the union") {
      val body = """{"MachineName":"m","ShoeSize":1,"MixingGapS1":"warm","WeightPercentageS1":4,"ShiftName":"Day"}"""

      assertTrue(optionalScalars.validateAndEncode(body).isLeft)
    },
    test("rejects a missing field, so a typo cannot quietly become a null column") {
      val body = """{"MachineName":"m","ShoeSize":1,"WeightPercentageS1":4,"ShiftName":"Day"}"""

      assertTrue(optionalScalars.validateAndEncode(body).isLeft)
    },
    test("rejects a malformed body") {
      assertTrue(optionalScalars.validateAndEncode("""{"MachineName":""").isLeft)
    },
    test("tags unions nested inside a record, not just the ones at the root") {
      val schema = compile(
        """{
          |  "type": "record", "name": "Envelope", "namespace": "com.sneaksanddata.pushstream",
          |  "fields": [
          |    { "name": "event_id", "type": ["null", "string"], "default": null },
          |    {
          |      "name": "payload",
          |      "type": {
          |        "type": "record", "name": "Payload",
          |        "fields": [{ "name": "MixingGapS1", "type": ["null", "double"], "default": null }]
          |      }
          |    }
          |  ]
          |}""".stripMargin
      )

      assertTrue(schema.validateAndEncode("""{"event_id":"e1","payload":{"MixingGapS1":0.35}}""").isRight)
    },
    test("tags unions inside arrays and maps") {
      val schema = compile(
        """{
          |  "type": "record", "name": "Containers", "namespace": "com.sneaksanddata.pushstream",
          |  "fields": [
          |    { "name": "readings", "type": { "type": "array", "items": ["null", "double"] } },
          |    { "name": "attributes", "type": { "type": "map", "values": ["null", "string"] } }
          |  ]
          |}""".stripMargin
      )

      assertTrue(schema.validateAndEncode("""{"readings":[0.5,null,2],"attributes":{"region":"dk"}}""").isRight)
    },
    test("picks the branch declared first when a value fits several") {
      // ["null", "long", "double"] reads 1 as a long, which is the choice avro itself makes when resolving schemas
      val schema = compile(
        """{
          |  "type": "record", "name": "Multi", "namespace": "com.sneaksanddata.pushstream",
          |  "fields": [{ "name": "reading", "type": ["null", "long", "double"], "default": null }]
          |}""".stripMargin
      )

      val asLong   = schema.validateAndEncode("""{"reading":1}""").toOption.get
      val explicit = schema.validateAndEncode("""{"reading":{"long":1}}""").toOption.get

      assertTrue(schema.validateAndEncode("""{"reading":1.5}""").isRight, asLong.sameElements(explicit))
    },
    test("tags a record branch by its full name, namespace included") {
      val schema = compile(
        """{
          |  "type": "record", "name": "Wrapper", "namespace": "com.sneaksanddata.pushstream",
          |  "fields": [
          |    {
          |      "name": "payload",
          |      "type": ["null", {
          |        "type": "record", "name": "Inner",
          |        "fields": [{ "name": "source", "type": "string" }]
          |      }],
          |      "default": null
          |    }
          |  ]
          |}""".stripMargin
      )

      assertTrue(schema.validateAndEncode("""{"payload":{"source":"integration-test"}}""").isRight)
    }
  )
