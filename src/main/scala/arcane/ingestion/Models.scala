package arcane.ingestion.Models
import java.time.OffsetDateTime

import zio.json._

type Payload = String

sealed trait AppError

// gzip/deflate will cause the request to be rejected
// the server decompression is not enabled
case class ContentEncodingError(text: String)            extends AppError
case class ConentLengthTooLargeError(acutalLength: Long) extends AppError
//  Content-Type application/json accepted only
case class ContentTypeError()                   extends AppError
case class NoContentError()                     extends AppError
case class AccessDeniedError()                  extends AppError
case class SchemaValidationError(cause: String) extends AppError
case class ParseError()                         extends AppError
