package arcane.ingestion.service

import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.rest.auth.OAuth2Properties
import zio.*
import zio.test.*
import zio.test.Assertion.*

import java.time.{Duration as JavaDuration}

object IcebergProvisionerSpec extends ZIOSpecDefault:

  def spec = suite("IcebergProvisionerLive.buildAdditionalPropertiesFromEnv")(
    test("returns empty properties when catalog no-auth flag is present") {
      val env = Map("ARCANE_FRAMEWORK__CATALOG_NO_AUTH" -> "1")

      assertTrue(IcebergProvisionerLive.buildAdditionalPropertiesFromEnv(env).isEmpty)
    },
    test("does not disable auth when catalog no-auth is explicitly false") {
      val env = Map(
        "ARCANE_FRAMEWORK__CATALOG_NO_AUTH"               -> "false",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_ID"     -> "client-id",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_SECRET" -> "client-secret",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI"    -> "https://auth.snd/token",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SCOPE"         -> "PRINCIPAL_ROLE:ALL"
      )

      assertTrue(IcebergProvisionerLive.buildAdditionalPropertiesFromEnv(env).nonEmpty)
    },
    test("builds OAuth2 credential properties from client id/secret") {
      val env = Map(
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_ID"     -> "client-id",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_SECRET" -> "client-secret",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI"    -> "https://auth.snd/token",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SCOPE"         -> "PRINCIPAL_ROLE:ALL"
      )

      val props = IcebergProvisionerLive.buildAdditionalPropertiesFromEnv(env)

      assertTrue(
        props(OAuth2Properties.CREDENTIAL) == "client-id:client-secret",
        props(OAuth2Properties.OAUTH2_SERVER_URI) == "https://auth.snd/token",
        props(OAuth2Properties.SCOPE) == "PRINCIPAL_ROLE:ALL",
        props(OAuth2Properties.TOKEN_REFRESH_ENABLED) == "false",
        props(CatalogProperties.AUTH_SESSION_TIMEOUT_MS) == JavaDuration.ofMinutes(55).toMillis.toString,
        !props.contains(OAuth2Properties.TOKEN)
      )
    },
    test("prefers static token when present") {
      val env = Map(
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI"             -> "https://auth.snd/token",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SCOPE"                  -> "PRINCIPAL_ROLE:ALL",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_STATIC_TOKEN"           -> "token-123",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_TOKEN_REFRESH_ENABLED"  -> "TRUE",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SESSION_TIMEOUT_MILLIS" -> "120000",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_ID"              -> "unused",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_SECRET"          -> "unused"
      )

      val props = IcebergProvisionerLive.buildAdditionalPropertiesFromEnv(env)

      assertTrue(
        props(OAuth2Properties.TOKEN) == "token-123",
        props(OAuth2Properties.OAUTH2_SERVER_URI) == "https://auth.snd/token",
        props(OAuth2Properties.SCOPE) == "PRINCIPAL_ROLE:ALL",
        props(OAuth2Properties.TOKEN_REFRESH_ENABLED) == "true",
        props(CatalogProperties.AUTH_SESSION_TIMEOUT_MS) == "120000",
        !props.contains(OAuth2Properties.CREDENTIAL)
      )
    },
    test("fails fast when required OAuth values are missing") {
      val env = Map(
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_ID"  -> "client-id",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI" -> "https://auth.snd/token",
        "ARCANE_FRAMEWORK__S3_CATALOG_AUTH_SCOPE"      -> "PRINCIPAL_ROLE:ALL"
      )

      assertZIO(ZIO.attempt(IcebergProvisionerLive.buildAdditionalPropertiesFromEnv(env)).exit)(
        fails(
          isSubtype[IllegalArgumentException](
            hasMessage(containsString("ARCANE_FRAMEWORK__S3_CATALOG_AUTH_CLIENT_SECRET"))
          )
        )
      )
    }
  )
