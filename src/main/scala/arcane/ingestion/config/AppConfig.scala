package arcane.ingestion.config

import zio.*
import zio.config.*
import zio.config.magnolia.{deriveConfig, discriminator, name}
import zio.config.yaml.YamlConfigProvider

import arcane.ingestion.common.ApplicationError
import arcane.ingestion.common.ApplicationError.*

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, NoSuchFileException, Path}

final case class ServerConfig(
    host: String = "0.0.0.0",
    port: Int = 8080,
    nThreads: Int = 8,
    maxContentLengthBytes: Long = 400L * 1024L
)

final case class RouterConfig(apiVersion: String = "v1")

/** Persistence backend configuration.
  *
  * Modelled as a sealed ADT so that DynamoDB and the in-memory provider are mutually exclusive by construction — the
  * YAML must select exactly one, and unrelated knobs cannot leak between environments. The zio-config-magnolia
  * `@discriminator("type")` annotation exposes the choice as a single YAML key (`type: dynamoDB` or `type: inMemory`),
  * which is the idiomatic way to express tagged unions in zio-config.
  */
@discriminator("type")
sealed trait PersistenceProvider

object PersistenceProvider:

  /** DynamoDB-backed persistence used in production. */
  @name("dynamoDB")
  final case class DynamoDB(
      region: String = "us-east-1",
      tableName: String = "arcane-ingestion",
      endpoint: Option[String] = None,
      autoCreateTable: Boolean = false
  ) extends PersistenceProvider

  /** In-memory persistence used for local dev and integration tests. Buffers records in a per-producer ZIO Queue.
    *   - `queueCapacity`: per-producer sliding queue capacity. When full, the oldest record is evicted so memory stays
    *     bounded even without a consumer.
    */
  @name("inMemory")
  final case class InMemory(
      queueCapacity: Int = 10000
  ) extends PersistenceProvider

/** DogStatsD-over-Unix-Domain-Socket publisher used by the arcane-framework `DataDog.UdsPublisher`.
  *
  *   - `enabled`: master switch. When `false`, no publisher fiber is started and metrics stay in-memory only.
  *   - `socketPath`: DogStatsD UDS socket path. Only read when `enabled = true`. Default matches the DataDog agent's
  *     standard install location.
  *   - `publisherIntervalSeconds`: how often the MetricsConfig flushes registered metrics to the socket.
  */
final case class DatadogConfig(
    enabled: Boolean = false,
    socketPath: String = "/var/run/datadog/dsd.socket",
    publisherIntervalSeconds: Long = 5
)

/** Observability wiring shared by logging and metrics.
  *
  *   - `serviceName`: becomes the `stream_kind` tag on every metric (matches arcane-framework semantics for
  *     `GlobalMetricTagProvider`) and is included in the SLF4J MDC.
  *   - `metricTags`: static key/value tags applied to every emitted metric, e.g. deployment/environment/region.
  *   - `datadog`: DataDog UDS publisher configuration; see [[DatadogConfig]].
  */
final case class ObservabilityConfig(
    serviceName: String = "arcane-ingestion",
    metricTags: Map[String, String] = Map.empty,
    datadog: DatadogConfig = DatadogConfig()
)

final case class AppConfig(
    server: ServerConfig = ServerConfig(),
    router: RouterConfig = RouterConfig(),
    persistence: PersistenceProvider = PersistenceProvider.DynamoDB(),
    observability: ObservabilityConfig = ObservabilityConfig()
)

/** Build AppConfig instance from various sources. The precedence (per-property):
  *   - CLI (highest, will override lower priority)
  *   - env vars
  *   - YAML file
  *   - defaults (lowest)
  *
  * Examples (with Prefix = ARCANE_INGESTION):
  *   - cli: ```--server\_\_n_threads -> server.nThreads```
  *   - env var: `ARCANE_INGESTION__SERVER__N_THREADS -> server.nThreads`
  */
object AppConfig {

  private val ConfigPathEnv    = "ARCANE_INGESTION_CONFIG"
  private val DefaultClasspath = "application.yaml"

  private val PathDelim: String = "__"
  private val EnvPrefix: String = sys.env.getOrElse("CONFIG_ENV_PREFIX", "ARCANE_INGESTION")

  private val config: Config[AppConfig] = deriveConfig[AppConfig]

  // Convenience wrapper, so it's easy to inject 'env-var provider' for testing
  private[config] def envProviderFor(env: Map[String, String]): ConfigProvider =
    ConfigProvider.fromMap(env, pathDelim = PathDelim).snakeCase.upperCase.nested(EnvPrefix)

  private[config] def argsProvider(args: ZIOAppArgs): ConfigProvider =
    ConfigProvider.fromAppArgs(args, pathDelim = PathDelim).snakeCase

  // Load `AppConfig` from prioritized config providers
  private[config] def loadFrom(
      args: ZIOAppArgs,
      env: Map[String, String],
      yaml: ConfigProvider
  ): IO[ApplicationError, AppConfig] =
    argsProvider(args)
      .orElse(envProviderFor(env))
      .orElse(yaml)
      .load(config)
      .mapError(e => ConfigurationParsingError(s"Invalid config structure: ${e.getMessage}", None))

  private val yamlProvider: ZIO[Any, ApplicationError, ConfigProvider] =
    ZIO
      .attemptBlocking {
        sys.env.get(ConfigPathEnv) match {
          case Some(path) => Some(Files.readString(Path.of(path), StandardCharsets.UTF_8))
          case None =>
            Option(getClass.getClassLoader.getResourceAsStream(DefaultClasspath)).map { in =>
              try new String(in.readAllBytes(), StandardCharsets.UTF_8)
              finally in.close()
            }
        }
      }
      .mapError {
        case e: NoSuchFileException   => ConfigurationError(s"Config file not found: ${e.getMessage}", Some(e))
        case e: FileNotFoundException => ConfigurationError(s"Config file not found: ${e.getMessage}", Some(e))
        case e                        => ConfigurationParsingError(s"Failed to load config: ${e.getMessage}", Some(e))
      }
      .flatMap {
        case Some(yaml) =>
          YamlConfigProvider
            .fromYamlStringZIO(yaml)
            .mapError(e => ConfigurationParsingError(s"Invalid YAML: ${e.getMessage}", Some(e)))
        case None => ZIO.succeed(ConfigProvider.fromMap(Map.empty))
      }

  val layer: ZLayer[ZIOAppArgs, ApplicationError, AppConfig] =
    ZLayer.fromZIO {
      for {
        args <- ZIO.service[ZIOAppArgs]
        yaml <- yamlProvider
        cfg  <- loadFrom(args, sys.env, yaml)
        _    <- ZIO.logInfo(s"Config loaded: ${cfg.server.host}:${cfg.server.port}")
      } yield cfg
    }
}
