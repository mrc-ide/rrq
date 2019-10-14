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


progress_timeout <- function(total, show, label, timeout, ...) {
  show <- show_progress(show)
  time_left <- time_checker(timeout)
  if (show) {
    single <- total == 1
    forever <- !is.finite(timeout)

    if (single) {
      ## Assume that we have the most simple pluralisation. This
      ## happens to work for all our cases, but is not generally true
      ## of course.
      label <- sub("s$", "", label)
      label_prefix <- sprintf("(:spin) waiting for %s", label)
    } else {
      label_prefix <- sprintf("(:spin) [:bar] :percent %s", label)
    }
    if (forever) {
      label_suffix <- "waited for :elapsed"
    } else {
      label_suffix <- "giving up in :remaining s"
    }
    fmt <- sprintf("%s | %s", label_prefix, label_suffix)
    p <- progress::progress_bar$new(fmt, total = total, show_after = 0,
                                    ...)$tick

    function(len = 1, ..., clear = FALSE) {
      rem <- max(0, time_left())
      move <- if (clear || rem == 0) total else if (single) 0L else len
      if (forever) {
        p(move)
      } else {
        width <- max(0, floor(log10(timeout))) + 1
        remaining <- formatC(rem, digits = 0, width = width, format = "f")
        p(move, tokens = list(remaining = remaining))
      }
      rem <= 0
    }
  } else {
    function(...) {
      time_left() <= 0
    }
  }
}


show_progress <- function(show) {
  show %||% getOption("rrq.progress", interactive())
}
