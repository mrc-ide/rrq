hostname <- function() {
  Sys.info()[["nodename"]]
}
process_id <- function() {
  Sys.getpid()
}
username <- function() {
  Sys.getenv("LOGNAME",
             Sys.getenv("USER",
                        Sys.getenv("LNAME",
                                   Sys.getenv("USERNAME"))))
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

l2c <- function(x) {
  vcapply(x, identity)
}

## TODO: After requiring 3.3.0, this can be dropped.
strrep <- function(x, times) {
  paste(rep(x, times), collapse = "")
}

blank <- function(n) {
  strrep(" ", n)
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

`%||%` <- function(a, b) {
  if (is.null(a)) b else a
}

is_directory <- function(path) {
  file.exists(path) && file.info(path, extra_cols = FALSE)[["isdir"]]
}

Sys_getenv <- function(x) {
  ret <- Sys.getenv(x)
  if (!nzchar(ret)) {
    stop(sprintf("Environment variable '%s' not set", x))
  }
  ret
}

lstrip <- function(x) {
  sub("^\\s+", "", x, perl = TRUE)
}
rstrip <- function(x) {
  sub("\\s+$", "", x, perl = TRUE)
}

## NOTE: duplicated from context, and will be replaced by using pathr
## once it's done.
is_absolute_path <- function(path) {
  grepl("^(/|[A-Za-z]:[/\\]|//|\\\\\\\\)", path)
}
is_relative_path <- function(path) {
  !is_absolute_path(path)
}

rbind_as_df <- function(x) {
  do.call("rbind",
          lapply(x, as.data.frame, stringsAsFactors = FALSE),
          quote = TRUE)
}
