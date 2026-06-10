package arcane.ingestion.config

import zio.*
import zio.config.yaml.YamlConfigProvider
import zio.test.*
import zio.test.Assertion.*

object AppConfigSpec extends ZIOSpecDefault {

  def spec = suite("AppConfig precedence")(
    test("CLI > env > YAML > defaults are merged per-property") {
      // YAML sets host + apiVersion + region.
      val yaml =
        """|server:
           |  port: 9092
           |  host: from-yaml
           |router:
           |  apiVersion: from-yaml-version
           |dynamodb:
           |  region: from-yaml-region
           |""".stripMargin

      // Env overrides apiVersion (wins over YAML) and sets tableName.
      val env = Map(
        "ARCANE_INGESTION__ROUTER__API_VERSION"  -> "from-env-version",
        "ARCANE_INGESTION__SERVER__PORT"         -> "9091",
        "ARCANE_INGESTION__DYNAMODB__TABLE_NAME" -> "from-env-table"
      )

      // CLI overrides port (wins over default), tableName (wins over env),
      // and apiVersion (wins over env+yaml).
      val args = ZIOAppArgs(
        Chunk(
          "--server__port=9090",
          "--router__api_version=from-args-version",
          "--dynamodb__table_name=from-args-table"
        )
      )

      for {
        yamlProvider <- YamlConfigProvider.fromYamlStringZIO(yaml)
        cfg          <- AppConfig.loadFrom(args, env, yamlProvider)
      } yield assertTrue(
        // CLI wins
        cfg.server.port == 9090,
        cfg.router.apiVersion == "from-args-version",
        cfg.dynamodb.tableName == "from-args-table",
        // YAML wins where neither CLI nor env set anything
        cfg.server.host == "from-yaml",
        cfg.dynamodb.region == "from-yaml-region",
        // Defaults survive where nothing was set
        cfg.server.nThreads == 8,
        cfg.server.maxContentLengthBytes == 400L * 1024L,
        cfg.dynamodb.autoCreateTable == false,
        cfg.dynamodb.endpoint.isEmpty
      )
    },
    test("env overrides YAML, defaults fill the rest (no CLI args)") {
      val yaml =
        """|server:
           |  host: from-yaml
           |  port: 7000
           |""".stripMargin

      val env = Map("ARCANE_INGESTION__SERVER__PORT" -> "9001")

      for {
        yamlProvider <- YamlConfigProvider.fromYamlStringZIO(yaml)
        cfg          <- AppConfig.loadFrom(ZIOAppArgs(Chunk.empty), env, yamlProvider)
      } yield assertTrue(
        cfg.server.host == "from-yaml",    // YAML
        cfg.server.port == 9001,           // env wins over YAML
        cfg.server.nThreads == 8,          // default
        cfg.router.apiVersion == "v1",     // default
        cfg.dynamodb.region == "us-east-1" // default
      )
    },
    test("all defaults apply when every source is empty") {
      for {
        yamlProvider <- YamlConfigProvider.fromYamlStringZIO("{}")
        cfg          <- AppConfig.loadFrom(ZIOAppArgs(Chunk.empty), Map.empty, yamlProvider)
      } yield assertTrue(cfg == AppConfig())
    }
  )
}
