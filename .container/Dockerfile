# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM sbtscala/scala-sbt:eclipse-temurin-25.0.1_8_1.12.9_3.8.3 AS build

WORKDIR /build

COPY . .

RUN --mount=type=secret,id=github_token sh -euc 'test -s /run/secrets/github_token || { echo "Missing BuildKit secret github_token (pass --secret id=github_token,env=GITHUB_TOKEN)" >&2; exit 1; }
GITHUB_TOKEN="$(cat /run/secrets/github_token)" sbt assembly'

FROM gcr.io/distroless/java25-debian13

COPY --from=build /build/target/com.sneaksanddata.arcane.stream-json.assembly.jar /app/

WORKDIR /app

CMD ["-jar", "/app/com.sneaksanddata.arcane.stream-json.assembly.jar"]
