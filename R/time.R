## Adapted from queuer
time_checker <- function(timeout) {
  stopifnot(is.numeric(timeout) && length(timeout == 1))
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units = "secs")
  if (is.finite(timeout)) {
    function() {
      as.double(timeout - (Sys.time() - t0), "secs")
    }
  } else {
    function() Inf
  }
}
