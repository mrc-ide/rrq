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
