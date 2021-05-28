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
