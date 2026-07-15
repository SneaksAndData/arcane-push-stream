FROM --platform=$BUILDPLATFORM sbtscala/scala-sbt:eclipse-temurin-25.0.1_8_1.12.9_3.8.3 AS build

WORKDIR /build

COPY . .

RUN --mount=type=secret,id=github_token GITHUB_TOKEN="$(cat /run/secrets/github_token)" sbt assembly

FROM gcr.io/distroless/java25-debian13

COPY --from=build /build/target/com.sneaksanddata.arcane.stream-json.assembly.jar /app/

WORKDIR /app

CMD ["-jar", "/app/com.sneaksanddata.arcane.stream-json.assembly.jar"]
