val zioVersion     = "2.1.24"
val zioHttpVersion = "3.11.2"

ThisBuild / dynverVTagPrefix := false
ThisBuild / dynverSeparator  := "-"

// arcane-framework-scala is published to a private GitHub Packages Maven repo.
// Pinning to the same coordinate the arcane-stream-pull plugin uses so the catalog
// behavior the ingestion service drives stays in lock-step with what the plugin sees.
resolvers += "Arcane framework repo" at "https://maven.pkg.github.com/SneaksAndData/arcane-framework-scala"

credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "_",
  sys.env.getOrElse(
    "GITHUB_TOKEN",
    sys.error("GITHUB_TOKEN must be set to resolve the arcane-framework dependency from GitHub Packages")
  )
)

lazy val root = project
  .in(file("."))
  .enablePlugins(BuildInfoPlugin, K8sCustomResourceCodegenPlugin)
  .settings(
    name             := "arcane-push-stream",
    organization     := "dev.zio",
    description      := "HTTP to ZIO streams web server",
    scalaVersion     := "3.8.3",
    buildInfoPackage := "arcane.ingestion",
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      version,
      "gitCommit" -> dynverGitDescribeOutput.value.map(_.commitSuffix.sha).getOrElse("unknown"),
      "buildTime" -> java.time.Instant.now.toString
    ),
    scalacOptions ++= Seq("-deprecation"),
    // zio-k8s-crd 3.1.2 generates `Implicits.scala` whose `implicit val show* = build[T](...)`
    // lines lack explicit type ascriptions. Scala 3.6+ rejects that as an error, and there is
    // no scalac flag to relax it. We patch the generated file in place — adding `: Show[T]` —
    // as part of the sources task, so the rest of the build (and our own code) stays on Scala
    // 3.8.3 with default strictness. The other warnings about `with`-as-type-operator and
    // trailing ` _` for eta-expansion are non-fatal.
    Compile / sources := {
      val srcs        = (Compile / sources).value
      val log         = streams.value.log
      val ImplicitDef = """implicit val (\w+) = build\[(.+?)\]""".r
      srcs.foreach { f =>
        if (f.getName == "Implicits.scala" && f.getAbsolutePath.contains("/src_managed/")) {
          val original = IO.read(f)
          val patched = ImplicitDef
            .replaceAllIn(original, m => s"implicit val ${m.group(1)}: Show[${m.group(2)}] = build[${m.group(2)}]")
          if (patched != original) {
            IO.write(f, patched)
            log.info(s"Patched zio-k8s codegen for Scala 3.6+ compat: ${f.getName}")
          }
        }
      }
      srcs
    },
    externalCustomResourceDefinitions := Seq(
      file(".helm/files/dataroute.yaml")
    ),
    libraryDependencies ++= Seq(
      "com.coralogix"  %% "zio-k8s-client" % "3.1.2",
      "org.apache.avro" % "avro"           % "1.11.4",
      // Pin SnakeYAML to 1.x so circe-yaml (used by zio-k8s-client to read kubeconfig) keeps working.
      // Several transitive deps (json-schema-validator historically, others) pull SnakeYAML 2.x,
      // whose SafeConstructor signature changed.
      "org.yaml"                       % "snakeyaml"             % "1.33",
      "com.softwaremill.sttp.client3" %% "slf4j-backend"         % "3.9.8",
      "org.slf4j"                      % "slf4j-simple"          % "2.0.16",
      "dev.zio"                       %% "zio"                   % zioVersion,
      "dev.zio"                       %% "zio-config"            % "4.0.7",
      "dev.zio"                       %% "zio-config-magnolia"   % "4.0.7",
      "dev.zio"                       %% "zio-config-yaml"       % "4.0.7",
      "dev.zio"                       %% "zio-http"              % zioHttpVersion,
      "dev.zio"                       %% "zio-json"              % "0.9.2",
      "dev.zio"                       %% "zio-schema"            % "1.8.5",
      "dev.zio"                       %% "zio-schema-derivation" % "1.8.5",
      "dev.zio"                       %% "zio-streams"           % zioVersion,
      "dev.zio"                       %% "zio-dynamodb"          % "1.0.0-RC25",

      // Iceberg catalog + entity-management primitives. Pinned to the same revision the
      // arcane-stream-pull plugin uses, so tables provisioned here are structurally
      // identical to what the plugin will read/write.
      "com.sneaksanddata" % "arcane-framework_3" % "2.2.1-91-g7a6b7d9",

      // Tests
      "dev.zio" %% "zio-http-testkit"    % zioHttpVersion % Test,
      "dev.zio" %% "zio-schema-zio-test" % "1.8.5"        % Test,
      "dev.zio" %% "zio-test"            % zioVersion     % Test,
      "dev.zio" %% "zio-test-sbt"        % zioVersion     % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    // Force SnakeYAML 1.x for circe-yaml compatibility (see comment above on library deps).
    dependencyOverrides += "org.yaml" % "snakeyaml" % "1.33",
    // Align Netty across zio-http (4.1.x) and AWS netty-nio-client (4.2.x).
    // In Netty 4.2 the monolithic `netty-codec` artifact was split into `netty-codec-base`
    // + `netty-codec-compression` (and others). If both 4.1 `netty-codec` and 4.2 `netty-codec-base`
    // are on the classpath, sbt-assembly fails with duplicate class entries (e.g.
    // io/netty/handler/codec/base64/Base64.class). Force everything to 4.2.x and drop the
    // obsolete `netty-codec` jar — its classes now live in the split artifacts.
    dependencyOverrides ++= Seq(
      "io.netty" % "netty-common"                       % "4.2.14.Final",
      "io.netty" % "netty-buffer"                       % "4.2.14.Final",
      "io.netty" % "netty-transport"                    % "4.2.14.Final",
      "io.netty" % "netty-transport-native-unix-common" % "4.2.14.Final",
      "io.netty" % "netty-resolver"                     % "4.2.14.Final",
      "io.netty" % "netty-handler"                      % "4.2.14.Final",
      "io.netty" % "netty-handler-proxy"                % "4.2.14.Final",
      "io.netty" % "netty-codec-base"                   % "4.2.14.Final",
      "io.netty" % "netty-codec-compression"            % "4.2.14.Final",
      "io.netty" % "netty-codec-http"                   % "4.2.14.Final",
      "io.netty" % "netty-codec-http2"                  % "4.2.14.Final"
    ),
    excludeDependencies += "io.netty" % "netty-codec",
    // Iceberg/SnowflakeJDBC (transitively via arcane-framework) pull the obsolete monolithic
    // `org.jline:jline:3.9.0`, which collides with the modular `jline-terminal`, `jline-reader`,
    // and `jline-terminal-jna` 3.19.x jars (they ship the same classes). Drop the monolithic
    // artifact; the split ones cover the same surface.
    excludeDependencies += "org.jline" % "jline",
    // needed to gracefully shutdown the server after `sbt run`
    run / fork         := true,
    run / connectInput := true,
    // assembly
    assembly / mainClass := Some("arcane.ingestion.Main"),
    // Put JAR in target/ directly, instead of in target/scala-x.x.x sub-directory
    assembly / assemblyOutputPath := target.value / (assembly / assemblyJarName).value,
    // We do not use the version name here, because it's executable file name
    // and we want to keep it consistent with the name of the project
    assembly / assemblyJarName := "com.sneaksanddata.arcane.push-stream.assembly.jar",
    assembly / assemblyMergeStrategy := {
      case "NOTICE"                                                                        => MergeStrategy.discard
      case "LICENSE"                                                                       => MergeStrategy.discard
      case ps if ps.contains("META-INF/services/java.net.spi.InetAddressResolverProvider") => MergeStrategy.discard
      case ps if ps.contains("META-INF/services/")                                         => MergeStrategy.concat("\n")
      case ps if ps.startsWith("META-INF/native")                                          => MergeStrategy.first

      // Removes duplicate files from META-INF
      // Mostly io.netty.versions.properties, license files, INDEX.LIST, MANIFEST.MF, etc.
      case ps if ps.startsWith("META-INF")         => MergeStrategy.discard
      case ps if ps.endsWith("logback.xml")        => MergeStrategy.first
      case ps if ps.endsWith("module-info.class")  => MergeStrategy.discard
      case ps if ps.endsWith("package-info.class") => MergeStrategy.discard

      // arcane-framework pulls multiple libs (zio-constraintless, zio-metrics-connectors-statsd, …)
      // that each ship their own auto-generated `buildinfo/BuildInfo` class at the root package.
      // Our own BuildInfo lives under `arcane.ingestion.BuildInfo`, so the unqualified ones are
      // safe to drop.
      case PathList("buildinfo", _*) => MergeStrategy.discard

      // unroll-annotation ships files that conflict with scala-library; prefer scala-library
      case PathList("scala", "annotation", _*) => MergeStrategy.first

      // Scala 3 derivation config files (e.g. from zio-json, circe-generic) — concatenate
      case "deriving.conf" => MergeStrategy.concat("\n")

      // for javax/activation and javax/xml package take the first one
      case PathList("javax", "activation", _*) => MergeStrategy.last
      case PathList("javax", "xml", _*)        => MergeStrategy.last

      // For other files we use the default strategy (deduplicate)
      case x => MergeStrategy.deduplicate
    }
  )
