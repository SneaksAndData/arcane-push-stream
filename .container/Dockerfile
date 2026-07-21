# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM sbtscala/scala-sbt:eclipse-temurin-25.0.1_8_1.12.9_3.8.3 AS build
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

WORKDIR /build

COPY . .

RUN sbt assembly

FROM gcr.io/distroless/java25-debian13

COPY --from=build /build/target/com.sneaksanddata.arcane.push-stream.assembly.jar /app/

WORKDIR /app

CMD ["--enable-native-access=ALL-UNNAMED", "-jar", "/app/com.sneaksanddata.arcane.push-stream.assembly.jar"]
