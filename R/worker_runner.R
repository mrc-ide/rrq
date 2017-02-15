rrq_worker_main <- function(args = commandArgs(TRUE)) {
  dat <- rrq_worker_main_args(args)
  con <- redux::hiredis(host = dat[["redis-host"]],
                        port = dat[["redis-port"]])
  context <- context::context_read(dat[["context_id"]], dat[["context_root"]])

  rrq_worker(context, con,
             key_alive   = dat[["key_alive"]],
             worker_name = dat[["worker_name"]],
             time_poll   = dat[["time_poll"]],
             log_path    = dat[["log-path"]],
             timeout     = dat[["timeout"]])
}

rrq_worker_main_args <- function(args) {
  message("Arguments:")
  message(paste(sprintf("- %s", args), collapse = "\n"))
  args <- context:::parse_command_args(args, "rrq_worker", 3:4)
  context_root <- args$root
  context_id <- args$args[[1L]]
  config_key <- args$args[[2L]]
  worker_name <- args$args[[3L]]
  key_alive <- if (args$n == 4L) args$args[[4L]] else NULL

  config <- worker_config_read(context_root, config_key)
  config$context_id <- context_id
  config$context_root <- context_root
  config$worker_name <- worker_name
  config$key_alive <- key_alive
  config
}

## TODO: This might become the primary way of launching workers?
## Rework things using it and see what it's like that way.  We can
## always tweak the rrq_worker functions a bit further to test if need
## be.
##
## TODO: is it ever useful to save the context_id into the config?  It
## seems that would lead to a proliferation of configurations and make
## it difficult to know when to save them.
rrq_worker_from_config <- function(root, context_id, worker_config,
                                   worker_name = NULL, key_alive = NULL) {
  context <- context::context_read(context_id, root)
  config <- worker_config_read(context, worker_config)
  con <- redux::hiredis(host = config[["redis-host"]],
                        port = config[["redis-port"]])
  rrq_worker(context, con,
             key_alive   = config[["key_alive"]],
             worker_name = config[["worker_name"]],
             time_poll   = config[["time_poll"]],
             log_path    = config[["log-path"]],
             timeout     = config[["timeout"]])
}
