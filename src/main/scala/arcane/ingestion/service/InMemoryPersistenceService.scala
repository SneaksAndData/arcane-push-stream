package arcane.ingestion.service

import arcane.ingestion.api.v1.SchemaRef
import arcane.ingestion.config.PersistenceProvider
import zio.*

import java.nio.charset.StandardCharsets
import java.time.{OffsetDateTime, ZoneOffset}
import java.util.UUID

/** Materialised representation of a single ingestion token, mirroring the row that the DynamoDB provider writes.
  *
  * `timestampUTC` is a lexicographically-ordered ISO-8601 UTC string, so draining a queue in order yields records in
  * chronological order — matching the semantics the arcane-stream-pull plugin relies on when polling DynamoDB with
  * `producer = X AND timestampUTC > $lastSeen`.
  */
final case class PersistedRecord(
    id: String,
    producer: String,
    timestampUTC: String,
    createdAt: Long,
    payload: String,
    schemaSubject: String,
    schemaVersion: Int,
    schemaFingerprint: Option[String]
)

/** In-memory [[PersistenceService]] that stores records in a ZIO `Queue` per producer.
  *
  * Intended as a drop-in replacement for the DynamoDB provider for local development, integration tests and demos —
  * *not* for production use. Each producer gets its own bounded sliding queue (oldest entries are dropped once
  * capacity is reached), which matches the DynamoDB partition-key isolation without unbounded memory growth when no
  * consumer is attached.
  *
  * The queue is a ZIO-native primitive, so back-pressure and inspection integrate naturally with the rest of the
  * service without introducing any extra concurrency machinery.
  */
trait InMemoryPersistenceService extends PersistenceService:

  /** Drains and returns every record currently buffered for `producer` (empty chunk if none). Useful for tests. */
  def drain(producer: String): UIO[Chunk[PersistedRecord]]

  /** Set of producers that have ever offered a record (queues are created lazily on first enqueue). */
  def producers: UIO[Set[String]]

final case class InMemoryPersistenceServiceLive(
    queues: Ref.Synchronized[Map[String, Queue[PersistedRecord]]],
    capacity: Int
) extends InMemoryPersistenceService:

  private def getOrCreateQueue(producer: String): UIO[Queue[PersistedRecord]] =
    queues.modifyZIO { m =>
      m.get(producer) match
        case Some(q) => ZIO.succeed((q, m))
        case None =>
          Queue.sliding[PersistedRecord](capacity).map(q => (q, m + (producer -> q)))
    }

  def enqueueToken(payload: Array[Byte], producer: String, schemaRef: SchemaRef): IO[Throwable, Boolean] =
    for
      id  <- ZIO.succeed(UUID.randomUUID().toString)
      now <- Clock.instant
      timestampUTC = OffsetDateTime.ofInstant(now, ZoneOffset.UTC).toString
      payloadJson  = new String(payload, StandardCharsets.UTF_8)
      record = PersistedRecord(
        id = id,
        producer = producer,
        timestampUTC = timestampUTC,
        createdAt = now.toEpochMilli,
        payload = payloadJson,
        schemaSubject = schemaRef.subject,
        schemaVersion = schemaRef.version,
        schemaFingerprint = schemaRef.fingerprint
      )
      q <- getOrCreateQueue(producer)
      // `Queue.sliding` never rejects an offer — it evicts the oldest element when full, so we always
      // report success. If we ever switch to `Queue.bounded` we should propagate the boolean instead.
      _ <- q.offer(record)
      _ <- ZIO.logDebug(
        s"[in-memory] stored record producer=$producer id=$id ts=$timestampUTC (${payloadJson.length} chars)"
      )
    yield true

  def drain(producer: String): UIO[Chunk[PersistedRecord]] =
    queues.get.flatMap { m =>
      m.get(producer) match
        case Some(q) => q.takeAll
        case None    => ZIO.succeed(Chunk.empty)
    }

  def producers: UIO[Set[String]] = queues.get.map(_.keySet)

object InMemoryPersistenceServiceLive:

  val live: ZLayer[PersistenceProvider.InMemory & ReadinessSignal, Nothing, PersistenceService] =
    ZLayer.fromZIO {
      for
        cfg       <- ZIO.service[PersistenceProvider.InMemory]
        readiness <- ZIO.service[ReadinessSignal]
        _         <- ZIO.logInfo(s"Initialising in-memory persistence: queueCapacity=${cfg.queueCapacity}")
        queues    <- Ref.Synchronized.make(Map.empty[String, Queue[PersistedRecord]])
        _         <- readiness.markReady
        _         <- ZIO.logInfo("Persistence ready — readiness signal set (in-memory provider)")
      yield InMemoryPersistenceServiceLive(queues, cfg.queueCapacity)
    }
