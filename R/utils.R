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

vcapply <- function(X, FUN, ...) { # nolint
  vapply(X, FUN, character(1), ...)
}

vnapply <- function(X, FUN, ...) { # nolint
  vapply(X, FUN, numeric(1), ...)
}

viapply <- function(X, FUN, ...) { # nolint
  vapply(X, FUN, integer(1), ...)
}

vlapply <- function(X, FUN, ...) { # nolint
  vapply(X, FUN, logical(1), ...)
}


blank <- function(n) {
  strrep(" ", n)
}

`%||%` <- function(a, b) { # nolint
  if (is.null(a)) b else a
}

`%&&%` <- function(a, b) { # nolint
  if (is.null(a)) a else b
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


list_to_numeric <- function(x) {
  vnapply(x, identity)
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

  n_total <- length(done)
  if (timeout > 0) {
    p <- progress_timeout(n_total, progress, name, timeout)
    tot <- sum(done)
    p$tick(tot)

    while (!all(done)) {
      sys_sleep(time_poll)

      prev <- tot
      done <- fetch()
      tot <- sum(done)

      if (p$tick(tot - prev)) {
        break
      }
    }
    p$terminate()
  }

  if (error && !all(done)) {
    remaining <- sum(!done)
    cli::cli_abort(
      "Exceeded maximum time ({remaining} / {n_total} {name}{?s} pending)")
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


is_call <- function(expr, what) {
  is.call(expr) && any(vlapply(what, identical, expr[[1L]]))
}


wait_timeout <- function(explanation, timeout, keep_going,
                         poll = timeout / 100, call = NULL) {
  t_end <- Sys.time() + timeout
  while (keep_going()) {
    if (Sys.time() > t_end) {
      cli::cli_abort("Timeout: {explanation}", call = call)
    }
    Sys.sleep(poll)
  }
}

wait_success <- function(explanation, timeout, keep_going,
                         poll = timeout / 5, call = NULL) {
  t_end <- Sys.time() + timeout
  out <- NULL
  while (is.null(out)) {
    out <- tryCatch(
      keep_going(),
      error = function(e) {
        if (Sys.time() > t_end) {
          cli::cli_abort("Timeout: {explanation}",
                         parent = e, call = call)
        } else {
          message(e$message)
          Sys.sleep(poll)
        }
      })
  }
  out
}


hash_data <- function(data) {
  rlang::hash(data)
}


write_bin <- function(object, path) {
  parent <- dirname(path)
  dir.create(parent, FALSE, TRUE)
  tmp <- tempfile(".rrq_object", parent)
  on.exit(unlink(tmp))
  writeBin(object, tmp)
  file.rename(tmp, path)
}


is_serialized_object <- function(x) {
  is.raw(x) &&
    length(x) >= 14 &&
    identical(x[1:2], as.raw(c(0x42, 0x0a)))
}


squote <- function(x) {
  sprintf("'%s'", x)
}


timestamp <- function(time = Sys.time()) {
  as.numeric(as.POSIXlt(time, tz = "UTC"))
}

na_drop <- function(x) {
  x[!is.na(x)]
}


first <- function(x) {
  x[[1L]]
}


last <- function(x) {
  x[[length(x)]]
}


df_rows <- function(d) {
  i <- vlapply(d, is.list)
  ret <- lapply(seq_len(nrow(d)), function(j) as.list(d[j, , drop = FALSE]))
  if (any(i)) {
    for (j in seq_along(ret)) {
      ret[[j]][i] <- lapply(ret[[j]][i], function(x) x[[1]])
    }
  }
  ret
}
