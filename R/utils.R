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


## TODO: After requiring 3.3.0, this can be dropped.
strrep <- function(x, times) {
  paste(rep(x, times), collapse = "")
}

blank <- function(n) {
  strrep(" ", n)
}

`%||%` <- function(a, b) {
  if (is.null(a)) b else a
}


sys_getenv <- function(x) {
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


set_names <- function(x, nms) {
  names(x) <- nms
  x
}


list_to_character <- function(x) {
  vcapply(x, identity)
}


data_frame <- function(...) {
  data.frame(..., stringsAsFactors = FALSE)
}


bin_to_object_safe <- function(x) {
  if (is.null(x)) NULL else bin_to_object(x)
}


sys_sleep <- function(n) {
  if (n > 0) {
    Sys.sleep(n)
  }
}


## To poll like this we want to know:
##
## how many things are currently done, so we need a function that
## returns a logical vector
general_poll <- function(fetch, time_poll, timeout, name, error, progress) {
  done <- fetch()

  if (timeout > 0) {
    p <- queuer::progress_timeout(length(done), show = progress,
                                  timeout = timeout, show_after = 0)
    tot <- sum(done)
    p(tot)

    while (!all(done)) {
      sys_sleep(time_poll)

      prev <- tot
      done <- fetch()
      tot <- sum(done)

      if (p(tot - prev)) {
        break
      }
    }
  }

  if (error && !all(done)) {
    stop(sprintf("Exceeded maximum time (%d / %d %s pending)",
                 sum(!done), length(done), name))
  }

  done
}


collector <- function(init = character(0)) {
  env <- new.env(parent = emptyenv())
  env$res <- init
  add <- function(x) {
    env$res <- c(env$res, x)
  }
  list(add = add,
       get = function() env$res)
}
