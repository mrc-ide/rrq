slowdouble <- function(x) {
  Sys.sleep(x)
  x * 2
}

f1 <- function(x) {
  x + 1
}

noisydouble <- function(x) {
  message("doubling ", x)
  x * 2
}

only_positive <- function(x) {
  if (x < 0) {
    stop("x must be positive")
  }
  x
}

warning_then_error <- function(x) {
  for (i in seq_len(x)) {
    warning("This is warning number ", i)
  }
  stop("Giving up now")
}


add <- function(a, b) {
  a + b
}


run_with_progress <- function(n, wait) {
  for (i in seq_len(n)) {
    rrq::rrq_task_progress_update(sprintf("iteration %d", i))
    Sys.sleep(wait)
  }
  n
}

run_with_progress_interactive <- function(path, poll = 0.01) {
  rrq::rrq_task_progress_update("Waiting for file")
  while (!file.exists(path)) {
    Sys.sleep(poll)
  }
  last_write <- ""
  repeat {
    contents <- tryCatch(readLines(path),
                         error = function(e) NULL)
    if (is.null(contents)) {
      next
    }
    if (identical(contents, "STOP")) {
      break
    }
    if (contents != last_write) {
      rrq::rrq_task_progress_update(sprintf("Got contents '%s'", contents))
      last_write <- contents
    }
    Sys.sleep(poll)
  }
  rrq::rrq_task_progress_update("Finishing")
  "OK"
}


run_with_progress_signal <- function(n, wait) {
  progress <- function(message) {
    signalCondition(structure(list(message = message),
                              class = c("progress", "condition")))
  }
  for (i in seq_len(n)) {
    progress(sprintf("iteration %d", i))
    Sys.sleep(wait)
  }
  n
}


dirty_double <- function(value) {
  prev <- .GlobalEnv$.rrq_dirty_double
  globalenv()$.rrq_dirty_double <- value
  list(prev, value * 2)
}


pid_and_sleep <- function(path, timeout) {
  writeLines(as.character(Sys.getpid()), path)
  Sys.sleep(timeout)
}
