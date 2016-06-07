hostname <- function() {
  Sys.info()[["nodename"]]
}
process_id <- function() {
  Sys.getpid()
}

vcapply <- function(X, FUN, ...) {
  vapply(X, FUN, character(1), ...)
}
vnapply <- function(X, FUN, ...) {
  vapply(X, FUN, numeric(1), ...)
}
viapply <- function(X, FUN, ...) {
  vapply(X, FUN, integer(1), ...)
}
vlapply <- function(X, FUN, ...) {
  vapply(X, FUN, logical(1), ...)
}

## TODO: After requiring 3.3.0, this can be dropped.
strrep <- function(x, times) {
  paste(rep(x, times), collapse="")
}

blank <- function(n) {
  strrep(" ", n)
}

## Copied from rrqueue; consider exposing?
progress_has_spin <- function() {
  packageVersion("progress") > numeric_version("1.0.2")
}

progress <- function(total, ..., show=TRUE, prefix="", fmt=NULL) {
  if (show) {
    if (is.null(fmt)) {
      fmt <- paste0(prefix,
                    if (progress_has_spin()) "(:spin) ",
                    "[:bar] :percent")
    }
    pb <- progress::progress_bar$new(fmt, total=total)
    pb_private <- environment(pb$tick)$private
    function(len=1, ..., clear=FALSE) {
      if (clear) {
        len <- pb_private$total - pb_private$current
      }
      invisible(pb$tick(len, ...))
    }
  } else {
    function(...) {}
  }
}

is_error <- function(x) {
  inherits(x, "try-error")
}

with_wd <- function(path, expr) {
  if (path != ".") {
    if (!file.exists(path)) {
      stop(sprintf("Path '%s' does not exist", path))
    }
    if (!is_directory(path)) {
      stop(sprintf("Path '%s' exists, but is not a directory", path))
    }
    owd <- setwd(path)
    on.exit(setwd(owd))
  }
  force(expr)
}

time_checker <- function(timeout, remaining=FALSE) {
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units="secs")
  if (remaining) {
    function() {
      as.double(timeout - (Sys.time() - t0), "secs")
    }
  } else {
    function() {
      Sys.time() - t0 > timeout
    }
  }
}

`%||%` <- function(a, b) {
  if (is.null(a)) b else a
}

dir_create <- function(paths) {
  for (p in unique(paths)) {
    dir.create(p, FALSE, TRUE)
  }
}

is_directory <- function(path) {
  file.exists(path) && file.info(path, extra_cols=FALSE)[["isdir"]]
}

Sys_getenv <- function(x) {
  ret <- Sys.getenv(x)
  if (!nzchar(ret)) {
    stop(sprintf("Environment variable '%s' not set", x))
  }
  ret
}

lstrip <- function(x) {
  sub("^\\s+", "", x, perl=TRUE)
}
rstrip <- function(x) {
  sub("\\s+$", "", x, perl=TRUE)
}
