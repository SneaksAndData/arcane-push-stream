package arcane.ingestion.Models
import java.time.OffsetDateTime

import zio.json._
import zio.schema.{DeriveSchema, Schema}
import zio.schema.annotation.description

type Payload = String

@description("Top-level application error type returned by the data ingestion endpoint")
sealed trait AppError
object AppError:
  given Schema[AppError] = DeriveSchema.gen[AppError]

// gzip/deflate will cause the request to be rejected
// the server decompression is not enabled
@description("Request used an unsupported Content-Encoding (decompression is disabled)")
case class ContentEncodingError(text: String) extends AppError
object ContentEncodingError:
  given Schema[ContentEncodingError] = DeriveSchema.gen[ContentEncodingError]

@description("Request body exceeds the maximum allowed Content-Length (400 kB)")
case class ConentLengthTooLargeError(acutalLength: Long) extends AppError
object ConentLengthTooLargeError:
  given Schema[ConentLengthTooLargeError] = DeriveSchema.gen[ConentLengthTooLargeError]

//  Content-Type application/json accepted only
@description("Only application/json Content-Type is accepted")
case class ContentTypeError() extends AppError
object ContentTypeError:
  given Schema[ContentTypeError] = DeriveSchema.gen[ContentTypeError]

@description("Content-Length header is required but missing")
case class LengthRequiredError() extends AppError
object LengthRequiredError:
  given Schema[LengthRequiredError] = DeriveSchema.gen[LengthRequiredError]

@description("Request body is missing or empty")
case class NoContentError() extends AppError
object NoContentError:
  given Schema[NoContentError] = DeriveSchema.gen[NoContentError]

@description("Caller is not authorised to invoke this endpoint")
case class AccessDeniedError() extends AppError
object AccessDeniedError:
  given Schema[AccessDeniedError] = DeriveSchema.gen[AccessDeniedError]

@description("Payload failed JSON Schema validation")
case class SchemaValidationError(cause: String) extends AppError
object SchemaValidationError:
  given Schema[SchemaValidationError] = DeriveSchema.gen[SchemaValidationError]

@description("Generic parse error")
case class ParseError() extends AppError
object ParseError:
  given Schema[ParseError] = DeriveSchema.gen[ParseError]

@description("Request body could not be deserialised")
case class SerializationError(cause: String) extends AppError
object SerializationError:
  given Schema[SerializationError] = DeriveSchema.gen[SerializationError]

@description("Persistence backend connection error (e.g. AWS client lost connection)")
case class ConnectionError(cause: String) extends AppError
object ConnectionError:
  given Schema[ConnectionError] = DeriveSchema.gen[ConnectionError]

@description("Persistence backend write error (e.g. DynamoDB rejected the put)")
case class DataWriteError(cause: String) extends AppError
object DataWriteError:
  given Schema[DataWriteError] = DeriveSchema.gen[DataWriteError]
