rrq_log <- function(topic, value) {
  n <- length(value) - 1L
  if (n > 0L) {
    topic <- c(topic, rep_len("...", n))
  }
  str <- trimws(sprintf("[ %-9s ]  %s", topic, value))
  if (n > 0L) {
    str <- paste(str, collapse = "\n")
  }
  message(str)
}
