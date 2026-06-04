package arcane.ingestion

import zio._
import zio.test._
import zio.test.Assertion._
import zio.http._
import zio.json._
import arcane.ingestion.service.TokenService
import java.util.concurrent.TimeUnit
import arcane.ingestion.Models.UserNotFoundError

object TokenServiceSuite extends ZIOSpecDefault {
  val getSuite = suite("gerOrCreateToken")(
    test("Concurrent access") {
      for {
        tokenService <- ZIO.service[TokenService]
        list         <- ZIO.foreachPar(1 to 100)(_ => tokenService.getOrCreateToken(1L))
      } yield assertTrue(list.distinct.size == 1)
    } @@ TestAspect.repeat(Schedule.recurs(10)),
    test("the same token for the same user") {
      for {
        tokenService <- ZIO.service[TokenService]
        t1           <- tokenService.getOrCreateToken(1)
        t2           <- tokenService.getOrCreateToken(1)
      } yield assertTrue(t1 == t2)
    },
    test("Token expiration") {
      for {
        tokenService <- ZIO.service[TokenService]
        t1           <- tokenService.getOrCreateToken(1)
        _            <- TestClock.adjust(Duration.apply(2, TimeUnit.HOURS))
        t2           <- tokenService.getOrCreateToken(1)
      } yield assertTrue(t1 != t2) && assertTrue(t2.expires.isAfter(t1.expires))
    }
  ).provide(ZLayer.fromZIO(TokenService()))

  val expireSuite = suite("expiredToken Suite")(
    test("checkExpired") {
      for {
        tokenService <- ZIO.service[TokenService]
        t1           <- tokenService.getOrCreateToken(1)
        _            <- TestClock.adjust(Duration.apply(2, TimeUnit.HOURS))
        result       <- tokenService.checkExpired(1, t1.uid)
      } yield assertTrue(result)
    },
    test("userNotFound") {
      val expired = for {
        tokenService <- ZIO.service[TokenService]
        t1           <- tokenService.getOrCreateToken(1)
        _            <- TestClock.adjust(Duration.apply(2, TimeUnit.HOURS))
        result       <- tokenService.checkExpired(2, t1.uid)
      } yield result
      assertZIO(expired.exit)(fails(equalTo(UserNotFoundError(2))))
    }
  ).provide(ZLayer.fromZIO(TokenService()))

  val allSuites = suite("Token Tests")(getSuite, expireSuite)

  def spec = allSuites
}
