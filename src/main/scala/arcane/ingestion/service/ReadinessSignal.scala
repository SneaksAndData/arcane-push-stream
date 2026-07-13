package arcane.ingestion.service

import zio.*

/** Process-wide readiness flag. Should be always ready after start-up.
  *
  * `true` after successful service initialisation, which is a pre-condition to accept traffic (currently: DynamoDB
  * connectivity).
  *
  * Since DynamoDB is currently being bootstrapped, it is best to wait until that is done, before we start serving
  * traffic. (To avoid HTTP 500 in case of starting a second pod.)
  */
trait ReadinessSignal:
  def markReady: UIO[Unit]
  def isReady: UIO[Boolean]

object ReadinessSignal:
  val live: ULayer[ReadinessSignal] =
    ZLayer.fromZIO {
      Ref.make(false).map { ref =>
        new ReadinessSignal:
          def markReady: UIO[Unit]  = ref.set(true)
          def isReady: UIO[Boolean] = ref.get
      }
    }

  def markReady: URIO[ReadinessSignal, Unit] =
    ZIO.serviceWithZIO[ReadinessSignal](_.markReady)

  def isReady: URIO[ReadinessSignal, Boolean] =
    ZIO.serviceWithZIO[ReadinessSignal](_.isReady)
