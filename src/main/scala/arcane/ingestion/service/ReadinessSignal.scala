package arcane.ingestion.service

import zio.*

/** Process-wide readiness flag. Flipped to `true` by subsystems whose successful initialisation is a precondition for
  * the service accepting traffic (currently: DynamoDB connectivity).
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
