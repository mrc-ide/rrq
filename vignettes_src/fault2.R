stochastic_failure <- function(p) {
  x <- runif(1)
  if (x < p) {
    stop(sprintf("Convergence failure - x is only %0.2f", x))
  }
  x
}
