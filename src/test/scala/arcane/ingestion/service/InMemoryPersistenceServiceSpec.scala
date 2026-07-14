package arcane.ingestion.service

import arcane.ingestion.api.v1.SchemaRef
import arcane.ingestion.config.PersistenceProvider
import zio.*
import zio.test.*
import zio.test.Assertion.*

object InMemoryPersistenceServiceSpec extends ZIOSpecDefault:

  private def bytes(s: String): Array[Byte] = s.getBytes(java.nio.charset.StandardCharsets.UTF_8)

  private val schema = SchemaRef(subject = "orders", version = 3, fingerprint = Some("fp-123"))

  private def inMemory: URIO[InMemoryPersistenceService, InMemoryPersistenceService] =
    ZIO.service[InMemoryPersistenceService]

  private val layer: ULayer[InMemoryPersistenceService] =
    ZLayer.make[InMemoryPersistenceService](
      ReadinessSignal.live,
      ZLayer.succeed(PersistenceProvider.InMemory(queueCapacity = 4)),
      ZLayer.fromZIO {
        for
          cfg       <- ZIO.service[PersistenceProvider.InMemory]
          readiness <- ZIO.service[ReadinessSignal]
          queues    <- Ref.Synchronized.make(Map.empty[String, Queue[PersistedRecord]])
          _         <- readiness.markReady
        yield InMemoryPersistenceServiceLive(queues, cfg.queueCapacity)
      }
    )

  def spec = suite("InMemoryPersistenceService")(
    test("enqueueToken stores records per producer with matching payload + schema metadata") {
      for
        svc  <- inMemory
        _    <- svc.enqueueToken(bytes("""{"a":1}"""), "producer-A", schema)
        _    <- svc.enqueueToken(bytes("""{"a":2}"""), "producer-A", schema)
        _    <- svc.enqueueToken(bytes("""{"b":1}"""), "producer-B", schema.copy(fingerprint = None))
        prod <- svc.producers
        a    <- svc.drain("producer-A")
        b    <- svc.drain("producer-B")
      yield assertTrue(
        prod == Set("producer-A", "producer-B"),
        a.length == 2,
        a.map(_.payload).toList == List("""{"a":1}""", """{"a":2}"""),
        a.forall(_.producer == "producer-A"),
        a.forall(_.schemaSubject == "orders"),
        a.forall(_.schemaVersion == 3),
        a.forall(_.schemaFingerprint.contains("fp-123")),
        b.length == 1,
        b.head.payload == """{"b":1}""",
        b.head.schemaFingerprint.isEmpty
      )
    },
    test("records are ordered chronologically by timestampUTC") {
      for
        svc <- inMemory
        _   <- ZIO.foreachDiscard(1 to 3)(i => svc.enqueueToken(bytes(s"""{"i":$i}"""), "ordered", schema))
        got <- svc.drain("ordered")
        stamps = got.map(_.timestampUTC).toList
      yield assertTrue(
        stamps == stamps.sorted,
        got.map(_.payload).toList == List("""{"i":1}""", """{"i":2}""", """{"i":3}""")
      )
    },
    test("sliding capacity evicts oldest records once full") {
      for
        svc <- inMemory
        // queueCapacity = 4 → offering 6 records drops the two oldest.
        _   <- ZIO.foreachDiscard(1 to 6)(i => svc.enqueueToken(bytes(s"""{"i":$i}"""), "sliding", schema))
        got <- svc.drain("sliding")
      yield assertTrue(
        got.length == 4,
        got.map(_.payload).toList == List("""{"i":3}""", """{"i":4}""", """{"i":5}""", """{"i":6}""")
      )
    },
    test("drain on an unknown producer returns empty chunk") {
      for
        svc <- inMemory
        got <- svc.drain("nobody-home")
      yield assertTrue(got.isEmpty)
    }
  ).provide(layer)
