rrq_lapply <- function(con, keys, db, X, FUN, DOTS, envir, envir_base,
                       timeout, time_poll, progress) {
  dat <- rrq_lapply_submit(con, keys, db, X, FUN, DOTS, envir, envir_base)
  if (timeout == 0) {
    return(dat)
  }
  rrq_bulk_wait(con, keys, dat, timeout, time_poll, progress)
}


rrq_lapply_submit <- function(con, keys, db, X, FUN, DOTS, envir, envir_base) {
  dat <- rrq_lapply_prepare(db, X, FUN, DOTS, envir, envir_base)
  key_complete <- rrq_key_task_complete(keys$queue)
  task_ids <- task_submit_n(con, keys, dat, key_complete)
  ret <- list(task_ids = task_ids, key_complete = key_complete,
              names = names(X))
  class(ret) <- "rrq_bulk"
  ret
}


rrq_lapply_prepare <- function(db, X, FUN, DOTS, envir, envir_base) {
  fun <- match_fun_envir(FUN, envir, envir_base)

  template <- as.call(c(list(fun$name, NULL), DOTS))
  dat <- expression_prepare(template, envir, envir_base, db,
                            function_value = if (is.null(fun$name)) fun$value)

  rewrite <- function(x) {
    dat$expr[[2L]] <- x
    object_to_bin(dat)
  }

  lapply(X, rewrite)
}


rrq_bulk_wait <- function(con, keys, dat, timeout, time_poll, progress,
                          delete = TRUE) {
  assert_is(dat, "rrq_bulk")
  ret <- tasks_wait(con, keys, dat$task_ids, timeout, time_poll, progress,
                    dat$key_complete)
  if (delete) {
    task_delete(con, keys, dat$task_ids, FALSE)
  }
  set_names(ret, dat$names)
}


## Try to work out what version of a function we are likely to have
## remotely.  Some of this could nodoubt be done with rlang, but with
## ~400 functions in that package and the crazy rate at which they
## deprecate and change I thin that would be more work in the long
## run.  This used to work quite well with lazyeval but that also
## changed behaviour and is itself basically deprecated in favour of
## rlang.
match_fun_envir <- function(fun, envir = parent.frame(), envir_base = NULL) {
  if (is_call(fun, quote(quote))) {
    fun <- fun[[2L]]
  }

  fun_search <- if (is.symbol(fun)) deparse(fun) else fun
  value <- match_fun(fun_search, envir)

  ## We can potentially do better here if the function belongs to a
  ## package namespace.
  name <- NULL
  if (is_namespaced_call(fun)) {
    name <- fun
  } else if (is.character(fun_search)) {
    name_ok <-
      identical(envir, envir_base) ||
      (!is.null(envir_base) &&
        identical(get(name_search, envir_base), value)) ||
      (is.primitive(value) &&
       identical(get(name_search, baseenv()), value))
    if (name_ok) {
      name <- if (is.character(fun)) as.name(fun) else fun
    }
  }

  return(list(name = name, value = value))
}


match_fun <- function(fun, envir) {
  if (is.function(fun)) {
    fun
  } else if (is.character(fun)) {
    get(fun, mode = "function", envir = envir)
  } else if (is_call(fun, quote(`::`))) {
    getExportedValue(deparse(fun[[2]]), deparse(fun[[3]]))
  } else if (is_call(fun, quote(`:::`))) {
    get(deparse(fun[[3]]), envir = asNamespace(deparse(fun[[2]])),
        mode = "function", inherits = FALSE)
  } else {
    stop("Could not find function")
  }
}


is_namespaced_call <- function(x) {
  is.call(x) && any(deparse(x[[1]]) == c("::", ":::"))
}
