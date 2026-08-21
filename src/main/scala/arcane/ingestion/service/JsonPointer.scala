package arcane.ingestion.service

import zio.json.ast.Json
import zio.json.DecoderOps

/** RFC 6901 JSON Pointer support, shared by the two places a route's `jsonExpressionPointer` is applied.
  *
  * At provisioning time [[IcebergProvisioner]] walks the pointer through the *Avro schema* to decide which record's
  * fields become table columns; at ingestion time [[extract]] walks the same pointer through each *request body* to
  * check that the document the consumer will look for is actually there. Both must agree on how a pointer is split and
  * unescaped, so they share [[segments]] — otherwise a pointer like `/a~1b` could select one thing in the table layout
  * and another in the data.
  *
  * The body itself is persisted whole; the pointer is applied again by arcane-stream-pull, which reads it back from the
  * table property the provisioner writes it to.
  */
object JsonPointer:

  /** Splits a pointer into its unescaped reference tokens. An absent, blank or empty pointer selects the document root
    * and therefore yields no tokens, which is what makes "no pointer configured" behave as "use the whole payload".
    */
  def segments(pointer: Option[String]): Seq[String] =
    pointer.map(_.trim).filter(_.nonEmpty) match
      case None => Seq.empty
      case Some(expression) =>
        if !expression.startsWith("/") then
          throw new IllegalArgumentException(
            s"jsonExpressionPointer '$expression' must be an RFC 6901 JSON Pointer starting with '/', e.g. '/payload'"
          )
        // `~1` and `~0` escape `/` and `~`; `~1` must be unescaped first, or an escaped `~` would be re-read as one
        expression.split("/", -1).drop(1).toSeq.map(_.replace("~1", "/").replace("~0", "~"))

  /** Resolves `pointer` against a JSON document, returning [[None]] when it selects nothing.
    *
    * A token addresses an object member by name or an array element by index. Anything else — a token against a scalar,
    * a non-numeric or out-of-range array index, a missing member — means the producer's document does not have the
    * shape the route was configured for, and the caller turns that into a rejected request.
    */
  def extract(body: String, pointer: Option[String]): Either[String, String] =
    val tokens = segments(pointer)
    if tokens.isEmpty then Right(body)
    else
      body.fromJson[Json] match
        case Left(error) => Left(s"payload is not valid JSON: $error")
        case Right(document) =>
          val resolved = tokens.foldLeft(Option(document)) {
            case (Some(Json.Obj(fields)), token) => fields.collectFirst { case (name, value) if name == token => value }
            case (Some(Json.Arr(elements)), token) => token.toIntOption.flatMap(elements.lift)
            case _                                 => None
          }
          resolved.map(_.toString).toRight(s"payload has no value at '${pointer.getOrElse("")}'")
