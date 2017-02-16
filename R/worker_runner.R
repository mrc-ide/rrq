rrq_worker_main <- function(args = commandArgs(TRUE)) {
  dat <- rrq_worker_main_args(args)
  rrq_worker_from_config(dat$root, dat$context_id, dat$worker_config,
                         dat$worker_name, dat$key_alive)
}

## until I fix the tests:
rrq_worker_main_args_old <- function(args) {
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

rrq_worker_main_args <- function(args) {
  message("Arguments:")
  message(paste(sprintf("- %s", args), collapse = "\n"))
  args <- context:::parse_command_args(args, "rrq_worker", 3:4)
  list(root = args$root,
       context_id = args$args[[1L]],
       worker_config = args$args[[2L]],
       worker_name = args$args[[3L]],
       key_alive = if (args$n == 4L) args$args[[4L]] else NULL)
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
  if (!interactive()) {
    context::context_log_start()
  }
  context <- context::context_read(context_id, root)
  config <- worker_config_read(context, worker_config)
  con <- redux::hiredis(host = config$redis_host, port = config$redis_port)
  rrq_worker(context, con,
             key_alive   = key_alive,
             worker_name = worker_name,
             time_poll   = config$time_poll,
             log_path    = config$log_path,
             timeout     = config$timeout)
}

write_rrq_worker <- function(root) {
  path <- context::context_root_get(root)$path
  context:::write_context_script(path, "rrq_worker", "rrq:::rrq_worker_main", 4:5)
}
