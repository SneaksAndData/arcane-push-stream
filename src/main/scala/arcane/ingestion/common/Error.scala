package arcane.ingestion.common

import zio.schema.DeriveSchema

case class ApiError(message: String, code: Int)
object ApiError {
  implicit val schema: zio.schema.Schema[ApiError] = DeriveSchema.gen[ApiError]
}

/** Base trait for all application-level errors. */
sealed trait ApplicationError extends Exception {
  def message: String
  override def getMessage: String = message
}

object ApplicationError {

  /** Raised when the TOML configuration file contains invalid syntax or missing keys. */
  case class ConfigurationParsingError(message: String, cause: Option[Throwable] = None) extends ApplicationError

  /** Raised when the configuration file cannot be loaded (e.g. file not found, permissions). */
  case class ConfigurationError(message: String, cause: Option[Throwable] = None) extends ApplicationError
}
