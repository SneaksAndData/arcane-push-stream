package arcane.ingestion.config

import zio._
import zio.config._
import com.moandjiezana.toml.Toml
import java.io.{File, FileNotFoundException}

import arcane.ingestion.common.ApplicationError
import arcane.ingestion.common.ApplicationError._

case class ServerConfig(host: String, port: Int, nThreads: Int, maxContentLengthBytes: Long)
case class RouterConfig(apiVersion: String)
case class DynamoDBConfig(
    region: String,
    tableName: String,
    endpoint: Option[String] = None,
    autoCreateTable: Boolean = false
)

case class AppConfig(server: ServerConfig, router: RouterConfig, dynamodb: DynamoDBConfig)

object AppConfig {

  private val EnvVar                       = "ARCANE_INGESTION_CONFIG"
  private val DefaultConfigurationFilePath = "application.toml"

  // Layer is an alias for ZLayer without any dependencies (i.e: ZLayer[Any, ..., ...])
  val layer: Layer[ApplicationError, AppConfig] =
    ZLayer {
      ZIO
        .succeed(Option(java.lang.System.getenv(EnvVar)))
        .tap {
          case Some(path) => ZIO.logInfo(s"Loading config from: $path")
          case None       => ZIO.logInfo(s"Loading config from classpath: $DefaultConfigurationFilePath")
        }
        .flatMap { source =>
          ZIO
            .attempt {
              source match {
                case Some(path) => new Toml().read(new File(path))
                case None => new Toml().read(getClass.getClassLoader.getResourceAsStream(DefaultConfigurationFilePath))
              }
            }
            .mapError {
              case e: FileNotFoundException =>
                ConfigurationError(s"Config file not found: ${e.getMessage}", Some(e))
              case e: IllegalStateException =>
                ConfigurationParsingError(s"Invalid TOML: ${e.getMessage}", Some(e))
              case e =>
                ConfigurationError(s"Failed to load config: ${e.getMessage}", Some(e))
            }
        }
        .flatMap { toml =>
          ZIO
            .attempt {
              AppConfig(
                server = ServerConfig(
                  host = toml.getString("server.host"),
                  port = toml.getLong("server.port").toInt,
                  nThreads = toml.getLong("server.nThreads").toInt,
                  maxContentLengthBytes =
                    if toml.contains("server.maxContentLengthBytes") then toml.getLong("server.maxContentLengthBytes")
                    else 400L * 1024L
                ),
                router = RouterConfig(
                  apiVersion = toml.getString("router.api_version")
                ),
                dynamodb = DynamoDBConfig(
                  region = toml.getString("dynamodb.region"),
                  tableName = toml.getString("dynamodb.tableName"),
                  endpoint = Option(toml.getString("dynamodb.endpoint")),
                  autoCreateTable =
                    if toml.contains("dynamodb.autoCreateTable") then toml.getBoolean("dynamodb.autoCreateTable")
                    else false
                )
              )
            }
            .mapError(e => ConfigurationParsingError(s"Invalid config structure: ${e.getMessage}", Some(e)))
        }
        .tap(cfg => ZIO.logInfo(s"Config loaded: ${cfg.server.host}:${cfg.server.port}"))
    }
}
