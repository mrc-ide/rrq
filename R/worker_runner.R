##' Write a small script that can be used to launch a rrq worker. The
##' resulting script takes the same arguments as the [rrq::rrq_worker]
##' constructor, but from the command line. See Details.
##'
##' If you need to launch rrq workers from a script, it's convenient
##' not to have to embed R code like:
##'
##' ```
##' Rscript -e 'rrq::rrq_worker$new("myqueue")'
##' ```
##'
##' as this is error-prone and unpleasant to quote and read. You can
##' use the function `rrq_worker_script` to write out a small helper
##' script which lets you write:
##'
##' ```
##' ./path/rrq_worker myqueue
##' ```
##'
##' instead.
##'
##' The helper script supports the same arguments as
##' the `[rrq::rrq_worker]` constructor:
##'
##' * `queue_id` as the sole positional argument
##' * `name_config` as `--config`
##' * `worker_id` as `--worker-id`
##'
##' To change the redis connection settings, set the `REDIS_URL`
##' environment variable (see [redux::hiredis()] for details).
##'
##' For example to create a worker `myworker` with configuration
##' `myconfig` on queue `myqueue` you might use
##'
##' ```
##' ./rrq_worker --config=myconfig --worker-id=myworker myqueue
##' ```
##'
##' @title Write worker runner script
##'
##' @param path The path to write to. Should be a directory (or one
##'   will be created if it does not yet exist). The final script will
##'   be `file.path(path, "rrq_worker")`
##'
##' @param versioned Logical, indicating if we should write a
##'   versioned R script that will use the same path to `Rscript` as
##'   the running session. If `FALSE` we use `#!/usr/bin/env Rscript`
##'   which will pick up `Rscript` from the path. You may want to use
##'   a versioned script in tests or if you have multiple R versions
##'   installed simultaneously.
##'
##' @return Invisibly, the path to the script
##'
##' @export
##' @examples
##' path <- rrq::rrq_worker_script(tempfile())
##' readLines(path)
rrq_worker_script <- function(path, versioned = FALSE) {
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

rrq_worker_main <- function(args = commandArgs(TRUE)) {
  dat <- rrq_worker_main_args(args)
  worker <- rrq_worker$new(dat$queue_id, dat$config, dat$worker_id)
  worker$loop()
  invisible()
}


rrq_worker_main_args <- function(args) {
  doc <- "Usage:
  rrq_worker [options] <id>

Options:
--config=NAME    Name of a worker configuration [default: localhost]
--worker-id=ID   Id of the worker (optional)"
  dat <- docopt::docopt(doc, args)
  names(dat) <- gsub("-", "_", names(dat), fixed = TRUE)
  list(queue_id = dat$id,
       config = dat$config,
       worker_id = dat$worker_id)
}


## This is required until didehpc is updated
write_rrq_worker <- function(...) {
  .Deprecated("rrq_worker_script")
  rrq_worker_script(...)
}
