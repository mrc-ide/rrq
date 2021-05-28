rrq_worker_from_config <- function(queue_id, worker_config = "localhost",
                                   worker_name = NULL, key_alive = NULL) {
  con <- redux::hiredis()
  keys <- rrq_keys(queue_id)
  config <- worker_config_read(con, keys, worker_config)

  rrq_worker$new(queue_id, con,
                 key_alive = key_alive,
                 worker_name = worker_name,
                 queue = config$queue,
                 time_poll = config$time_poll,
                 timeout = config$timeout,
                 heartbeat_period = config$heartbeat_period,
                 verbose = config$verbose)
}


rrq_worker_main <- function(args = commandArgs(TRUE)) {
  dat <- rrq_worker_main_args(args)
  worker <- rrq_worker_from_config(dat$queue_id, dat$config, dat$name,
                                   dat$key_alive)
  worker$loop()
  invisible()
}


rrq_worker_main_args <- function(args) {
  doc <- "Usage:
  rrq_worker [options] <id>

Options:
--config=NAME    Name of a worker configuration [default: localhost]
--name=NAME      Name of the worker (optional)
--key-alive=KEY  Key to write to once alive (optional)"
  dat <- docopt::docopt(doc, args)
  names(dat) <- gsub("-", "_", names(dat), fixed = TRUE)
  list(queue_id = dat$id,
       config = dat$config,
       name = dat$name,
       key_alive = dat[["key_alive"]])
}


## Adopted from orderly:
write_rrq_worker <- function(path = tempfile(), versioned = FALSE) {
  dir.create(path, FALSE, TRUE)
  if (versioned) {
    rscript <- file.path(R.home(), "bin", "Rscript")
  } else {
    rscript <- "/usr/bin/env Rscript"
  }
  code <- c(sprintf("#!%s", rscript),
            "rrq:::rrq_worker_main()")
  path_bin <- file.path(path, "rrq_worker")
  writeLines(code, path_bin)
  Sys.chmod(path_bin, "755")
  invisible(path_bin)
}
