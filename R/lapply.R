## TODO: roll together with queuer

## NOTE: this is largely duplicated from the queuer::qlapply but
## tweaked a bit.  I'll be rolling these together perhaps at some
## point.
##
## The difference is that we go through and prepare the *template*
## once (including copying locals around), then go through and queue
## the prepared expressions.  This is a good approach to use in queuer
## too.  The workers will do a reasonable job of not reloading locals.
rrq_lapply_submit <- function(obj, X, FUN, envir, ...) {
  con <- obj$con
  keys <- obj$keys
  db <- obj$db
  XX <- as.list(X)
  n <- length(XX)
  DOTS <- lapply(lazyeval::lazy_dots(...), "[[", "expr")
  key_complete <- rrq_key_task_complete(keys$queue_name)

  fun_dat <- queuer::match_fun_queue(FUN, envir, obj$envir)

  if (is.null(fun_dat$name_symbol)) {
    hash <- db$set_by_value(fun_dat$value, "rrq_functions")
    fun <- as.name(hash)
  } else {
    fun <- fun_dat$name_symbol
    hash <- NULL
  }

  template <- as.call(c(list(fun), list(NULL), DOTS))
  dat <- prepare_expression(template, envir, obj$envir, db, hash)
  f <- function(x) {
    dat$expr[[2L]] <- x
    object_to_bin(dat)
  }
  list(key_complete=key_complete,
       task_ids=task_submit_n(con, keys, lapply(XX, f), key_complete),
       hash=hash)
}

rrq_lapply_collect <- function(obj, dat, nms,
                               timeout=Inf, time_poll=NULL, progress_bar=TRUE) {
  con <- obj$con
  keys <- obj$keys
  task_ids <- dat$task_ids
  key_complete <- dat$key_complete

  ## NOTE: Given that this will throw and the ids will be lost
  ## forever, it probably makes sense to try and flush the queue at
  ## this point, or implement various error handling approaches.
  ret <- collect_wait_n(con, keys, task_ids, key_complete,
                        timeout=timeout, time_poll=time_poll,
                        progress_bar=progress_bar)
  tasks_delete(con, keys, task_ids, FALSE)
  if (!is.null(dat$hash)) {
    obj$db$del(dat$hash, "rrq_functions")
  }
  setNames(ret, nms)
}

rrq_lapply <- function(obj, X, FUN, ..., envir=parent.frame(),
                       timeout=Inf, time_poll=NULL, progress_bar=TRUE) {
  dat <- rrq_lapply_submit(obj, X, FUN, envir, ...)
  rrq_lapply_collect(obj, dat, names(X), timeout, time_poll, progress_bar)
}
