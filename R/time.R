## Adapted from queuer
time_checker <- function(timeout, remaining = FALSE) {
  stopifnot(is.numeric(timeout) && length(timeout == 1))
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units = "secs")
  if (is.finite(timeout)) {
    if (remaining) {
      function() {
        as.double(timeout - (Sys.time() - t0), "secs")
      }
    } else {
      function() {
        Sys.time() - t0 > timeout
      }
    }
  } else {
    if (remaining) {
      function() Inf
    } else {
      function() FALSE
    }
  }
}
