package arcane.ingestion.service

import zio._
import arcane.ingestion.Models._
import arcane.ingestion.service.PersistenceService

trait RequestService:
  def enqueueToken(payload: String, producer: String): IO[Throwable, Boolean]

case class RequestServiceLive(persistenceService: PersistenceService) extends RequestService {

  def enqueueToken(payload: String, producer: String): IO[Throwable, Boolean] =
    persistenceService.enqueueToken(payload, producer)

}

object RequestServiceLive:
  val live: ZLayer[PersistenceService, Nothing, RequestService] =
    ZLayer.fromFunction(RequestServiceLive(_))
