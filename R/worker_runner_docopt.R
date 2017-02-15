rrq_worker_main_docopt <- function(args = commandArgs(TRUE)) {
  'Usage:
  rrq_worker --context-root=ROOT [options]

  Options:
  --context-root=ROOT  Context root (required)
  --context-id=ID      Context id (required)
  --redis-host=IP      Redis host [default: 127.0.0.1]
  --redis-port=PORT    Redis port [default: 6379]
  --key-alive=KEY      Key to write to after worker comes alive
  --time-poll=TIME     Time (in seconds) to poll queues
  --worker-name=NAME   Optional name to use for the worker
  --timeout=TIME       Optional worker timeout (in seconds)
  --log-path=PATH      Optional path for logs
' -> doc

  args <- docopt::docopt(doc, args)

  context_root <- args[["context-root"]]
  context_id <- args[["context-id"]]
  context::context_log_start()

  worker_name <- args[["worker-name"]]
  time_poll <- docopt_number(args, "time_poll", formals(rrq_worker)$time_poll)
  timeout <- docopt_number(args, "timeout")

  context <- context::context_read(context_id, context_root)
  ## TODO: This interacts badly with having set REDIS_URL!  Decide on
  ## sensible defaults.
  con <- redux::hiredis(host = args[["redis-host"]],
                        port = args[["redis-port"]])

  rrq_worker(context, con, key_alive = args[["key-alive"]],
             worker_name = worker_name, time_poll = time_poll,
             log_path = args[["log-path"]], timeout = timeout)
}

docopt_number <- function(args, name, default = NULL) {
  x <- args[[name]]
  if (is.null(x)) {
    default
  } else {
    h <- function(e) {
      stop(sprintf("while processing %s:\n\t",
                   name, e$message), call. = FALSE)
    }
    withCallingHandlers(as.numeric(x), warning = h)
  }
}
