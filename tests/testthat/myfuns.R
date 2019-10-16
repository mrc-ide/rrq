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
    rrq::rrq_progress_update(sprintf("iteration %d", i))
    Sys.sleep(wait)
  }
  n
}


run_with_progress_interactive <- function(path, poll = 0.01) {
  rrq::rrq_progress_update("Waiting for file")
  while (!file.exists(path)) {
    Sys.sleep(poll)
  }
  repeat {
    contents <- readLines(path)
    if (identical(contents, "STOP")) {
      break
    }
    rrq::rrq_progress_update(sprintf("Got contents '%s'", contents))
    Sys.sleep(poll)
  }
  rrq::rrq_progress_update("Finishing")
}
