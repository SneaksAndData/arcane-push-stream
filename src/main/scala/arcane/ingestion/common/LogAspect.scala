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

  /** Best-effort client IP: prefers proxy/ingress-set headers (requests reach the app behind the k8s ingress) and
    * falls back to the socket's remote address.
    */
  def clientIp(req: Request): String =
    req.headers
      .get("X-Forwarded-For")
      .map(_.split(",").head.trim)
      .orElse(req.headers.get("X-Real-IP"))
      .orElse(req.remoteAddress.map(_.getHostAddress))
      .getOrElse("unknown")

  /** Annotates the current log span with the requesting method, path and client IP, so every log line emitted while
    * handling the request (including the `<label>_ms` duration annotation added by `logSpan`) carries "who hit which
    * endpoint, how fast" without needing a full APM tracer wired in.
    */
  def logAnnotateRequestContext(
      req: Request
  ): ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any] =
    new ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any] {
      override def apply[R, E, A](
          zio: ZIO[R, E, A]
      )(implicit trace: Trace): ZIO[R, E, A] =
        ZIO.logAnnotate(
          Set(
            LogAnnotation("client-ip", clientIp(req)),
            LogAnnotation("http-method", req.method.toString),
            LogAnnotation("http-path", req.path.toString)
          )
        )(zio)
    }
}
