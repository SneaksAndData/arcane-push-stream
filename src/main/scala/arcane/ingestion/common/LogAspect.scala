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

  /** Best-effort client IP: prefers proxy/ingress-set headers (requests reach the app behind the k8s ingress) and falls
    * back to the socket's remote address.
    */
  def clientIp(req: Request): String =
    req.headers
      .get("X-Forwarded-For")
      .map(_.split(",").head.trim)
      .orElse(req.headers.get("X-Real-IP"))
      .orElse(req.remoteAddress.map(_.getHostAddress))
      .getOrElse("unknown")

  /** The correlation id to trace a request by: the caller's own if it supplied one, otherwise a fresh id.
    *
    * Resolved once per request by [[annotateRequestContext]] and annotated for the whole of it, so a generated id is
    * shared by every line the request produces. Resolving it more than once would mint a second id and split one
    * request's lines into two unrelated traces.
    */
  private def correlationId(req: Request): UIO[String] =
    ZIO
      .succeed(req.headers.get("X-Correlation-ID"))
      .flatMap(id => Random.nextUUID.map(uuid => id.getOrElse(uuid.toString)))

  /** The annotations describing one request: which endpoint was called, by which method, from where, under which
    * correlation id.
    */
  private def requestAnnotations(req: Request): UIO[Set[LogAnnotation]] =
    correlationId(req).map { id =>
      Set(
        LogAnnotation("correlation-id", id),
        LogAnnotation("client-ip", clientIp(req)),
        LogAnnotation("http-method", req.method.toString),
        LogAnnotation("http-path", req.path.toString)
      )
    }

  /** Annotates every line a request produces with its endpoint, method, client IP and correlation id.
    *
    * This is a middleware rather than a per-route aspect because an aspect can only reach the lines the route's own
    * handler writes. The access log line comes from `Middleware.requestLogging`, which runs outside any handler, so
    * annotating from within a route left exactly the one line that is emitted for *every* request — including requests
    * that matched no route — without a path on it.
    *
    * Applied outermost, it covers the access log line, the handler's own lines and the not-found handler alike, so a
    * single `http-path` filter returns everything that happened while serving a request.
    */
  def annotateRequestContext: Middleware[Any] = new Middleware[Any] {
    override def apply[Env, Err](routes: Routes[Env, Err]): Routes[Env, Err] =
      routes.transform[Env] { handler =>
        // the handler's scope has to stay open until its response body is consumed, so it is carried through rather
        // than closed here: `Handler.scoped` re-attaches it to the surrounding server request scope
        Handler.scoped[Env](
          Handler.fromFunctionZIO[Request] { request =>
            requestAnnotations(request).flatMap(annotations => ZIO.logAnnotate(annotations)(handler(request)))
          }
        )
      }
  }
}
