rrq_worker_main <- function(args = commandArgs(TRUE)) {
  dat <- rrq_worker_main_args(args)
  worker <- rrq_worker_from_config(dat$root, dat$context_id, dat$worker_config,
                                   dat$worker_name, dat$key_alive)
  worker$loop()
  invisible()
}

rrq_worker_main_args <- function(args) {
  message("Arguments:")
  message(paste(sprintf("- %s", args), collapse = "\n"))
  dat <- context::parse_context_args(args, "rrq_worker", 3:4)
  list(root = dat$root,
       context_id = dat$args[[1L]],
       worker_config = dat$args[[2L]],
       worker_name = dat$args[[3L]],
       key_alive = if (dat$n == 4L) dat$args[[4L]] else NULL)
}

rrq_worker_from_config <- function(queue_id, worker_config = "localhost",
                                   worker_name = NULL, key_alive = NULL) {
  con <- redux::hiredis()
  keys <- rrq_keys(queue_id)
  config <- worker_config_read(con, keys, worker_config)

  R6_rrq_worker$new(con, queue_id,
                    key_alive = key_alive,
                    worker_name = worker_name,
                    time_poll = config$time_poll,
                    log_path = config$log_path,
                    timeout = config$timeout,
                    heartbeat_period = config$heartbeat_period)
}

write_rrq_worker <- function(root) {
  path <- context::context_root_get(root)$path
  context::write_context_script(path, "rrq_worker",
                                "rrq:::rrq_worker_main", 4:5)
}
