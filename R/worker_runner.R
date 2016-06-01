rrq_worker_main <- function(args=commandArgs(TRUE)) {
  'Usage:
  rrq_worker --context-root=ROOT [options]

  --context-root=ROOT  Context root (required)
  --context-id=ID      Context id (may be required)
  --redis-host=IP      Redis host [default: 127.0.0.1]
  --redis-port=PORT    Redis port [default: 6379]
  --key-alive=KEY      Key to write to after worker comes alive
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
  context <- context::context_handle(context_root, context_id)
  con <- redux::hiredis(host=args[["redis-host"]], port=args[["redis-port"]])
  rrq_worker(context, con, args[["key-alive"]])
}
