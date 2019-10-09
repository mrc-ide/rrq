rrq_lapply <- function(con, keys, db, X, FUN, DOTS, envir, envir_base,
                       timeout, time_poll, progress) {
  dat <- rrq_lapply_submit(con, keys, db, X, FUN, DOTS, envir, envir_base)
  if (timeout == 0) {
    return(dat)
  }
  rrq_bulk_wait(con, keys, dat, timeout, time_poll, progress)
}


rrq_lapply_prepare <- function(db, X, FUN, DOTS, envir, envir_base = NULL) {
  ## for the moment, require the function to be language and we won't
  ## check for it at all!
  stopifnot(is.language(FUN))
  function_name <- FUN

  template <- as.call(c(list(function_name, NULL), DOTS))
  dat <- expression_prepare(template, envir, envir_base, db)

  rewrite <- function(x) {
    dat$expr[[2]] <- x
    object_to_bin(dat)
  }

  lapply(X, rewrite)
}


rrq_lapply_submit <- function(con, keys, db, X, FUN, DOTS, envir, envir_base) {
  dat <- rrq_lapply_prepare(db, X, FUN, DOTS, envir)
  key_complete <- rrq_key_task_complete(keys$queue)
  task_ids <- task_submit_n(con, keys, dat, key_complete)
  ret <- list(task_ids = task_ids, key_complete = key_complete,
              names = names(X))
  class(ret) <- "rrq_bulk"
  ret
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
  fun_lazy <- substitute(fun)
  if (is_call(fun_lazy, quote(quote))) {
    fun_lazy <- fun_lazy[[2L]]
  }

  if (is.symbol(fun_lazy)) {
    fun_forced <- deparse(fun_lazy)
  } else {
    fun_forced <- force(fun)
  }

  ## This ensures that we actually have a function that we can use in
  ## all cases, though the the value found here is not used in all
  ## branches below
  fun_value <- match_fun(fun_forced, envir)

  if (is_namespaced_call(fun_lazy)) {
    return(list(value = fun_lazy, type = "name"))
  }

  if (is.character(fun_forced)) {
    name <- fun_forced
    if (!is.null(envir_base) && identical(get(name, envir_base), fun_value)) {
      return(list(value = as.name(name), type = "name"))
    } else {
      return(list(value = fun_value, type = "value"))
    }
  }

  return(list(value = fun_value, type = "value"))
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
