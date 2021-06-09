create <- function(n) {
  saveRDS(runif(n), "numbers.rds")
}

use <- function(i) {
  d <- readRDS("numbers.rds")
  d[[i]]
}
