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
