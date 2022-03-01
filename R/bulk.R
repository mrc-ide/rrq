rrq_lapply <- function(con, keys, store, x, fun, dots, envir, queue,
                       separate_process, task_timeout, depends_on,
                       collect_timeout, time_poll, progress) {
  dat <- rrq_bulk_submit(con, keys, store, x, fun, dots, FALSE, envir, queue,
                         separate_process, task_timeout, depends_on)
  if (collect_timeout == 0) {
    return(dat)
  }
  rrq_bulk_wait(con, keys, store, dat, collect_timeout, time_poll, progress)
}


rrq_enqueue_bulk <- function(con, keys, store, x, fun, dots,
                             envir, queue, separate_process, task_timeout,
                             depends_on, collect_timeout, time_poll, progress) {
  dat <- rrq_bulk_submit(con, keys, store, x, fun, dots, TRUE,
                         envir, queue, separate_process, task_timeout,
                         depends_on)
  if (collect_timeout == 0) {
    return(dat)
  }
  rrq_bulk_wait(con, keys, store, dat, collect_timeout, time_poll, progress)
}


rrq_bulk_submit <- function(con, keys, store, x, fun, dots, do_call,
                            envir, queue, separate_process, task_timeout,
                            depends_on) {
  fun <- match_fun_envir(fun, envir)
  n <- if (do_call && is.data.frame(x)) nrow(x) else length(x)
  task_ids <- ids::random_id(n)

  if (do_call) {
    x <- rrq_bulk_prepare_call_x(x)
    dat <- rrq_bulk_prepare_call(store, task_ids, x, fun, dots, envir)
  } else {
    dat <- rrq_bulk_prepare_lapply(store, task_ids, x, fun, dots, envir)
  }

  key_complete <- rrq_key_task_complete(keys$queue_id)
  task_submit_n(con, keys, store, task_ids, dat, key_complete, queue,
                separate_process, task_timeout,
                depends_on = depends_on)
  ret <- list(task_ids = task_ids,
              key_complete = key_complete,
              names = names(x))
  class(ret) <- "rrq_bulk"
  ret
}


## As in lapply(X, FUN, ...) => [FUN(X[[1]], ...), FUN(X[[2]], ...), ...]
rrq_bulk_prepare_lapply <- function(store, task_ids, x, fun, dots, envir) {
  template <- as.call(c(list(fun$name, NULL), dots))
  rewrite <- function(x, dat) {
    dat$expr[2L] <- x
    object_to_bin(dat)
  }
  dat <- expression_prepare(template, envir, store, task_ids,
                            function_value = if (is.null(fun$name)) fun$value)
  lapply(x, rewrite, dat)
}


## Bulk lapply(X, FUN, ...) => [FUN(X[[1]][[1]], X[[1]][[2]], ...),
##                              FUN(X[[2]][[2]], X[[2]][[2]], ...),
##                              ...]
rrq_bulk_prepare_call <- function(store, task_ids, x, fun, dots, envir) {
  len <- length(x[[1L]])
  nms <- names(x[[1L]])
  args <- set_names(rep(list(NULL), len), nms)
  template <- as.call(c(list(fun$name), args, dots))
  idx <- seq_len(len) + 1L

  dat <- expression_prepare(template, envir, store, task_ids,
                            function_value = if (is.null(fun$name)) fun$value)
  rewrite <- function(x) {
    dat$expr[idx] <- x
    if (!is.null(nms)) {
      names(dat$expr[idx]) <- nms
    }
    object_to_bin(dat)
  }
  lapply(x, rewrite)
}


rrq_bulk_prepare_call_x <- function(x) {
  is_factor <- is.factor(x) || (is.recursive(x) && any(vlapply(x, is.factor)))
  if (is_factor) {
    stop("Factors cannot be used in bulk expressions")
  }

  ## Simplifies logic below
  if (is.atomic(x) && !is.null(x)) {
    x <- as.list(x)
  }

  if (is.data.frame(x)) {
    if (ncol(x) == 0L) {
      stop("'x' must have at least one column")
    }
    if (nrow(x) == 0L) {
      stop("'x' must have at least one row")
    }
    x <- df_to_list(x)
  } else if (is.list(x)) {
    if (length(x) == 0L) {
      stop("'x' must have at least one element")
    }
    lens <- lengths(x)
    if (length(unique(lens)) != 1L) {
      stop("Every element of 'x' must have the same length")
    }
    nms <- lapply(x, names)
    if (!all(vlapply(nms, identical, nms[[1]]))) {
      stop("Elements of 'x' must have the same names")
    }
  } else {
    stop("x must be a data.frame or list")
  }

  x
}


rrq_bulk_wait <- function(con, keys, store, dat, timeout, time_poll, progress,
                          delete = TRUE) {
  assert_is(dat, "rrq_bulk")
  ret <- tasks_wait(con, keys, store, dat$task_ids, timeout,
                    time_poll, progress, dat$key_complete)
  if (delete) {
    task_delete(con, keys, store, dat$task_ids, FALSE)
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
match_fun_envir <- function(fun, envir = parent.frame()) {
  while (is_call(fun, quote(quote))) {
    fun <- fun[[2L]]
  }

  if (is_call(fun, quote(`function`))) {
    fun <- eval(fun, envir)
  }

  fun_search <- if (is.symbol(fun)) deparse(fun) else fun
  value <- match_fun(fun_search, envir)

  ## We can potentially do better here if the function belongs to a
  ## package namespace.
  name <- NULL
  if (is_namespaced_call(fun)) {
    name <- fun
  } else if (is.character(fun_search)) {
    ## NOTE: This can be done as a big set of X || Y || Z clauses but
    ## it's easier for bugs to hide there because it's not so obvious
    ## that each branch has been run.  So this is done as an if/else
    ## ladder here at least for now.
    if (is.primitive(value) && identical(get(fun_search, baseenv()), value)) {
      name_ok <- TRUE
    } else {
      name_ok <- FALSE
    }
    if (name_ok) {
      name <- if (is.character(fun)) as.name(fun) else fun
    }
  }

  list(name = name, value = value)
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
