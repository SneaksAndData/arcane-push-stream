package arcane.ingestion.service
import zio.*
import zio.http.*
import zio.http.endpoint.*
import zio.json._
import zio.schema.{Schema, DeriveSchema}

trait HealthService:

  def health: ZIO[Any, Nothing, Health]

final case class Health(status: String, seed: Int)
object Health:
  given Schema[Health]      = DeriveSchema.gen[Health]
  given JsonEncoder[Health] = DeriveJsonEncoder.gen[Health]

final case class Ready(status: String, seed: Int) derives JsonEncoder

object HealthService extends HealthService:
  // TODO: implement queue service check
  override def health: ZIO[Any, Nothing, Health] = Random.nextInt.map(n => Health("ok", n))

  val live: ZLayer[Any, Nothing, HealthService] =
    ZLayer.succeed(HealthService)
