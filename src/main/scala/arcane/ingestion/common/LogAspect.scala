package arcane.ingestion.common

import zio._
import zio.http._

object LogAspect {
  def logSpan(
      label: String
  ): ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any] =
    new ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any] {
      override def apply[R, E, A](zio: ZIO[R, E, A])(implicit
          trace: Trace
      ): ZIO[R, E, A] =
        ZIO.logSpan(label)(zio)
    }

  def logAnnotateCorrelationId(
      req: Request
  ): ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any] =
    new ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any] {
      override def apply[R, E, A](
          zio: ZIO[R, E, A]
      )(implicit trace: Trace): ZIO[R, E, A] =
        correlationId(req).flatMap(id => ZIO.logAnnotate("correlation-id", id)(zio))

      def correlationId(req: Request): UIO[String] =
        ZIO
          .succeed(req.headers.get("X-Correlation-ID"))
          .flatMap(id => Random.nextUUID.map(uuid => id.getOrElse(uuid.toString)))
    }
}
