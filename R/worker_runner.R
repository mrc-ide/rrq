rrq_worker_main <- function(args=commandArgs(TRUE)) {
  'Usage:
  rrq_worker --context-root=ROOT [options]

  Options:
  --context-root=ROOT  Context root (required)
  --context-id=ID      Context id (may be required)
  --redis-host=IP      Redis host [default: 127.0.0.1]
  --redis-port=PORT    Redis port [default: 6379]
  --key-alive=KEY      Key to write to after worker comes alive
  --time-poll=TIME     Time (in seconds) to poll queues
  --worker-name=NAME   Optional name to use for the worker
  --timeout=TIME       Optional worker timeout (in seconds)
' -> doc

  args <- docopt::docopt(doc, args)

  context_root <- args[["context-root"]]
  context_id <- args[["context-id"]]

  if (is.null(context_id)) {
    ids <- context::contexts_list(context_root)
    if (length(ids) == 1L) {
      context_id <- ids
    } else if (length(ids) == 0L) {
      stop("No contexts found in ", context_root)
    } else {
      stop(sprintf("More than 1 contexts founds in %s; pick one of:\n%s",
                   context_root, paste0("\t-", ids, collapse="\n")))
    }
  }
  worker_name <- args[["worker-name"]]
  time_poll <- args[["time-poll"]] %||% formals(rrq_worker)$time_poll
  timeout <- args[["timeout"]]

  context <- context::context_handle(context_root, context_id)
  con <- redux::hiredis(host=args[["redis-host"]], port=args[["redis-port"]])

  rrq_worker(context, con, key_alive=args[["key-alive"]],
             worker_name=worker_name, time_poll=time_poll, timeout=timeout)
}
