package arcane.ingestion.service

import zio._
import arcane.ingestion.Models._

trait QueueService:
  def enqueueToken(payload: String, producer: String): ZIO[Any, Any, Boolean]

case class QueueServiceLive() extends QueueService {

  /// connect to aws dynamodb
  private def makeConnection(): UIO[Boolean] = ???

  def enqueueToken(payload: String, producer: String): UIO[Boolean] =
    ZIO.succeed(true)

}

object QueueServiceLive:
  val live: ZLayer[Any, Nothing, QueueService] =
    ZLayer.succeed(QueueServiceLive())
