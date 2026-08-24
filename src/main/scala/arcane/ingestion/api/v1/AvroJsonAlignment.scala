package arcane.ingestion.api.v1

import org.apache.avro.Schema
import zio.Chunk
import zio.json.ast.Json

import scala.jdk.CollectionConverters.*

/** Rewrites a producer's plain JSON document into the tagged form Avro's JSON decoder insists on.
  *
  * Avro's JSON encoding requires every union value to be wrapped in a single-member object naming the branch it belongs
  * to — `{"MixingGapS1": {"double": 0.35}}` rather than `{"MixingGapS1": 0.35}` — because a bare `5` cannot say whether
  * it meant the `int`, `long` or `double` branch of the union it sits in. That is an artefact of how Avro serializes,
  * not of what the producer is describing, and pushing it onto every caller of the ingestion API makes the wire format
  * surprising to write by hand and awkward to generate from an ordinary JSON serializer.
  *
  * So the tag is inferred here instead, from the branch whose type the value already fits, and the document handed to
  * the decoder is the tagged one. The consumer side has always worked this way: the framework's `AvroJsonDecoder` wraps
  * values before decoding them, so accepting plain JSON here makes the two ends agree.
  *
  * Alignment never makes an invalid document valid. It only adds tags the decoder would otherwise have demanded: a
  * value that fits no branch is left exactly as it is, so Avro still rejects it and reports why. Missing fields,
  * unknown fields and wrong types are all still the decoder's business.
  */
private[v1] object AvroJsonAlignment:

  /** Aligns `document` against `schema`, returning it with union branches tagged.
    *
    * Recurses through records, arrays and maps so a union nested anywhere in the document is tagged too, not just the
    * ones at the root.
    */
  def align(document: Json, schema: Schema): Json = schema.getType match
    case Schema.Type.UNION  => alignUnion(document, schema)
    case Schema.Type.RECORD => alignRecord(document, schema)
    case Schema.Type.ARRAY  => alignArray(document, schema)
    case Schema.Type.MAP    => alignMap(document, schema)
    case _                  => document

  /** Aligns each declared field of a record, leaving anything the schema does not declare untouched so the decoder can
    * still complain about it.
    */
  private def alignRecord(document: Json, schema: Schema): Json = document match
    case Json.Obj(fields) =>
      Json.Obj(fields.map { case (name, value) =>
        Option(schema.getField(name)) match
          case Some(field) => name -> align(value, field.schema())
          case None        => name -> value
      })
    case other => other

  private def alignArray(document: Json, schema: Schema): Json = document match
    case Json.Arr(elements) => Json.Arr(elements.map(align(_, schema.getElementType)))
    case other              => other

  private def alignMap(document: Json, schema: Schema): Json = document match
    case Json.Obj(fields) =>
      Json.Obj(fields.map { case (name, value) => name -> align(value, schema.getValueType) })
    case other => other

  /** Tags a union value, unless it is already tagged or fits no branch at all.
    *
    * `null` is the one union value Avro writes bare, so it passes straight through.
    */
  private def alignUnion(document: Json, union: Schema): Json =
    val branches = union.getTypes.asScala.toSeq

    document match
      case Json.Null                            => Json.Null
      case tagged if isTagged(tagged, branches) => alignTagged(tagged, branches)
      case value =>
        branches.find(branch => fits(value, branch)) match
          // recurse before wrapping: a record branch may itself hold unions that need tagging
          case Some(branch) => Json.Obj(Chunk(branch.getFullName -> align(value, branch)))
          // no branch accepts this value, so leave it alone and let the decoder produce the error
          case None => value

  /** Whether a value already carries a branch tag.
    *
    * The check is deliberately narrow — a single member whose name is exactly one of the branch names — because a
    * record branch that happens to declare one field could otherwise be mistaken for a tag. It stays ambiguous in the
    * one case where a union holds a record whose single field is named after a sibling branch; there the document is
    * read as already tagged.
    */
  private def isTagged(document: Json, branches: Seq[Schema]): Boolean = document match
    case Json.Obj(Chunk((name, _))) => branches.exists(_.getFullName == name)
    case _                          => false

  /** Descends into an already-tagged value so unions deeper in the document are still aligned. */
  private def alignTagged(document: Json, branches: Seq[Schema]): Json = document match
    case Json.Obj(Chunk((name, value))) =>
      branches.find(_.getFullName == name) match
        case Some(branch) => Json.Obj(Chunk(name -> align(value, branch)))
        case None         => document
    case other => other

  /** Whether a JSON value can be read as `branch`, using the same latitude Avro's own decoder allows.
    *
    * Numbers are the only interesting case: a whole number can be read as any numeric type, while a fractional one can
    * only be a `float` or a `double`. Declaration order therefore decides between equally valid branches, which is why
    * a union like `["null", "long", "double"]` reads `1` as a `long` — the same choice Avro makes when resolving writer
    * against reader schemas.
    */
  private def fits(document: Json, branch: Schema): Boolean = (document, branch.getType) match
    case (Json.Str(_), Schema.Type.STRING | Schema.Type.ENUM | Schema.Type.BYTES | Schema.Type.FIXED) => true
    case (Json.Bool(_), Schema.Type.BOOLEAN)                                                          => true
    case (Json.Num(value), Schema.Type.INT | Schema.Type.LONG) => value.stripTrailingZeros.scale() <= 0
    case (Json.Num(_), Schema.Type.FLOAT | Schema.Type.DOUBLE) => true
    case (Json.Obj(_), Schema.Type.RECORD | Schema.Type.MAP)   => true
    case (Json.Arr(_), Schema.Type.ARRAY)                      => true
    case _                                                     => false
