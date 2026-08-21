package arcane.ingestion.service

import zio.*
import zio.test.*
import zio.test.Assertion.*

/** Covers how a route's `jsonExpressionPointer` selects the document that gets persisted. What this returns has to
  * match, field for field, the columns [[IcebergProvisionerLive]] derives from the same pointer — otherwise
  * arcane-stream-pull decodes the stored document against a table it does not fit.
  */
object JsonPointerSpec extends ZIOSpecDefault:

  private val body =
    """{"id":"abc","payload":{"eventType":"click","sequence":42},"tags":["a","b"],"a/b":{"escaped":true}}"""

  def spec = suite("JsonPointer.extract")(
    test("persists the whole body when no pointer is configured") {
      assertTrue(JsonPointer.extract(body, None) == Right(body))
    },
    test("persists the whole body for an empty pointer") {
      assertTrue(JsonPointer.extract(body, Some("")) == Right(body))
    },
    test("persists only the pointed-at object") {
      assertTrue(JsonPointer.extract(body, Some("/payload")) == Right("""{"eventType":"click","sequence":42}"""))
    },
    test("follows more than one segment") {
      assertTrue(JsonPointer.extract(body, Some("/payload/eventType")) == Right("\"click\""))
    },
    test("addresses an array element by index") {
      assertTrue(JsonPointer.extract(body, Some("/tags/1")) == Right("\"b\""))
    },
    test("unescapes a segment containing a slash") {
      assertTrue(JsonPointer.extract(body, Some("/a~1b")) == Right("""{"escaped":true}"""))
    },
    test("fails when the pointer names a missing member") {
      assert(JsonPointer.extract(body, Some("/missing")))(isLeft(containsString("no value at '/missing'")))
    },
    test("fails when an array index is out of range") {
      assert(JsonPointer.extract(body, Some("/tags/9")))(isLeft(anything))
    },
    test("fails when a segment is applied to a scalar") {
      assert(JsonPointer.extract(body, Some("/id/nested")))(isLeft(anything))
    },
    test("fails when the body is not valid JSON") {
      assert(JsonPointer.extract("not json", Some("/payload")))(isLeft(containsString("not valid JSON")))
    },
    test("rejects a pointer that does not start with a slash") {
      assertZIO(ZIO.attempt(JsonPointer.extract(body, Some("payload"))).exit)(
        fails(isSubtype[IllegalArgumentException](hasMessage(containsString("RFC 6901"))))
      )
    }
  )
