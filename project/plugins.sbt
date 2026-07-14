addSbtPlugin("org.scalameta"    % "sbt-scalafmt"         % "2.5.2")
addSbtPlugin("com.eed3si9n"     % "sbt-assembly"         % "2.3.1")
addSbtPlugin("org.scoverage"    % "sbt-scoverage"        % "2.4.4")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")
addSbtPlugin("com.github.sbt"   % "sbt-native-packager"  % "1.11.7")
addSbtPlugin("com.eed3si9n"     % "sbt-buildinfo"        % "0.13.1")
addSbtPlugin("com.github.sbt"   % "sbt-dynver"           % "5.1.0")
addSbtPlugin("com.coralogix"    % "zio-k8s-crd"          % "3.1.2")

// zio-k8s-crd 3.1.2 runs Twilio guardrail (com.twilio:guardrail_2.12:0.64.1) to generate the
// custom-resource models. Guardrail was compiled against scalameta 4.4.15, which still ships
// `scala.meta.classifiers.Aliases`. Newer scalameta (e.g. parsers 4.17.x, dragged onto the shared
// sbt plugin classpath by other plugins) drops/relocates that class, leaving a mixed scalameta
// classpath. Guardrail's interpreter then fails to load with
// `java.lang.NoClassDefFoundError: scala/meta/classifiers/Aliases` and the Docker build breaks
// during `Compile / managedSources` (the CRD codegen step).
//
// Pin every scalameta module to guardrail's expected 4.4.15 so the CRD codegen is deterministic
// regardless of what version transitive plugin dependencies would otherwise select. (The actual
// scalafmt formatting engine is pinned separately via `version` in .scalafmt.conf and is loaded
// by sbt-scalafmt in an isolated classloader, so it is unaffected by this pin.)
dependencyOverrides ++= Seq(
  "org.scalameta" %% "scalameta"   % "4.4.15",
  "org.scalameta" %% "parsers"     % "4.4.15",
  "org.scalameta" %% "trees"       % "4.4.15",
  "org.scalameta" %% "common"      % "4.4.15",
  "org.scalameta" %% "dialects"    % "4.4.15",
  "org.scalameta" %% "inputs"      % "4.4.15",
  "org.scalameta" %% "tokens"      % "4.4.15",
  "org.scalameta" %% "tokenizers"  % "4.4.15",
  "org.scalameta" %% "quasiquotes" % "4.4.15",
  "org.scalameta" %% "io"          % "4.4.15"
)
