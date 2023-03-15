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


## TODO: After requiring 3.3.0, this can be dropped.
strrep <- function(x, times) {
  paste(rep(x, times), collapse = "")
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
    p <- progress_timeout(length(done), progress, name, timeout)
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


is_call <- function(expr, what) {
  is.call(expr) && any(vlapply(what, identical, expr[[1L]]))
}


df_to_list <- function(x) {
  at <- attributes(x)
  attributes(x) <- at[intersect(names(at), c("names", "class", "row.names"))]

  i <- vlapply(x, is.list)
  prepare <- function(el) {
    el <- as.list(el)
    el[i] <- lapply(el[i], unlist, FALSE)
    el
  }
  unname(lapply(split(x, seq_len(nrow(x))), prepare))
}


wait_timeout <- function(explanation, timeout, keep_going,
                         poll = timeout / 100) {
  t_end <- Sys.time() + timeout
  while (keep_going()) {
    if (Sys.time() > t_end) {
      stop("Timeout: ", explanation)
    }
    Sys.sleep(poll)
  }
}

wait_success <- function(explanation, timeout, keep_going,
                         poll = timeout / 5) {
  t_end <- Sys.time() + timeout
  out <- NULL
  while (is.null(out)) {
    out <- tryCatch(
      keep_going(),
      error = function(e) {
        if (Sys.time() > t_end) {
          e$message <- sprintf("Timeout: %s\n%s", explanation,
                               e$message)
          stop(e)
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
