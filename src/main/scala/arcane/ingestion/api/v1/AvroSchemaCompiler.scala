package arcane.ingestion.api.v1

import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import zio.json.DecoderOps
import zio.json.ast.Json

import java.io.ByteArrayOutputStream
import scala.util.Try

/** A parsed Avro schema bundled with helpers for the request hot-path: validate an incoming JSON payload against the
  * schema and re-encode it as Avro binary.
  *
  * Producers send ordinary JSON: `{"name": "x"}` for a field declared `["null", "string"]`. Avro's own JSON encoding
  * would require the union branch to be named — `{"name": {"string": "x"}}` — but that tag is inferred by
  * [[AvroJsonAlignment]] before decoding, so the wire format stays what any JSON serializer would produce. Documents
  * that are already tagged keep working, since alignment leaves an existing tag in place.
  *
  * `fingerprint` is the lower-case hex CRC-64-AVRO parsing fingerprint of the schema. It uniquely identifies the schema
  * bytes (ignoring whitespace and field order in JSON) and is suitable for use as a content-addressed schema id.
  */
final class CompiledAvroSchema(val schema: Schema):

  val fingerprint: String =
    SchemaNormalization
      .parsingFingerprint("CRC-64-AVRO", schema)
      .map("%02x".format(_))
      .mkString

  /** Decode `jsonPayload` against schema (validation) and re-encode the resulting record as Avro binary. Returns
    * `Left(error)` if the JSON is malformed or does not conform to the schema.
    *
    * The payload is aligned first, which supplies the union branch tags Avro's JSON decoder requires but ordinary JSON
    * does not carry. Alignment only ever adds tags, so a document that does not match the schema still fails here.
    *
    * Both reader and writer are instantiated per call to avoid sharing mutable state across concurrent requests.
    */
  def validateAndEncode(jsonPayload: String): Either[Throwable, Array[Byte]] =
    Try {
      val reader  = new GenericDatumReader[GenericRecord](schema)
      val decoder = DecoderFactory.get().jsonDecoder(schema, alignToSchema(jsonPayload))
      val record  = reader.read(null.asInstanceOf[GenericRecord], decoder)

      val writer  = new GenericDatumWriter[GenericRecord](schema)
      val baos    = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(baos, null)
      writer.write(record, encoder)
      encoder.flush()
      baos.toByteArray
    }.toEither

  /** Adds union branch tags to `jsonPayload`. A body that is not valid JSON is passed through untouched, so the parse
    * error is reported by the decoder along with every other malformed-payload error rather than from here.
    */
  private def alignToSchema(jsonPayload: String): String =
    jsonPayload.fromJson[Json] match
      case Right(document) => AvroJsonAlignment.align(document, schema).toString
      case Left(_)         => jsonPayload

/** Parses Avro schema text (the canonical JSON representation of a schema) once at route-load time. Cached per-route in
  * [[RouteLoader]] so request handling pays only the per-call decode/encode cost.
  */
object AvroSchemaCompiler:
  def compile(schemaText: String): Either[Throwable, CompiledAvroSchema] =
    Try(new CompiledAvroSchema(new Schema.Parser().parse(schemaText))).toEither
