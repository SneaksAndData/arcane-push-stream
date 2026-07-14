package arcane.ingestion.service

import zio._
import arcane.ingestion.Models._
import arcane.ingestion.api.v1.SchemaRef
import arcane.ingestion.service.PersistenceService

trait RequestService:
  def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean]

case class RequestServiceLive(persistenceService: PersistenceService) extends RequestService {

  def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean] =
    persistenceService.enqueueToken(payload, producer, schemaRef)

}

object RequestServiceLive:
  val live: ZLayer[PersistenceService, Nothing, RequestService] =
    ZLayer.fromFunction(RequestServiceLive(_))
