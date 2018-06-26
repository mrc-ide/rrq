rrq_enqueue_bulk <- function(obj, X, FUN, ..., DOTS = NULL, do_call = FALSE,
                             envir = parent.frame(), use_names = TRUE,
                             timeout = Inf, time_poll = NULL, progress = NULL) {
  dat <- rrq_enqueue_bulk_submit(obj, X, FUN, ..., DOTS = DOTS,
                                 do_call = do_call,
                                 envir = envir, use_names = use_names)
  rrq_enqueue_bulk_wait(obj, dat, timeout, time_poll, progress)
}

rrq_lapply <- function(obj, X, FUN, ..., DOTS = NULL,
                       envir = parent.frame(), use_names = TRUE,
                       timeout = Inf, time_poll = NULL, progress = NULL) {
  rrq_enqueue_bulk(obj, X, FUN, ..., DOTS = DOTS, do_call = FALSE,
                   envir = envir, use_names = use_names,
                   timeout = timeout, time_poll = time_poll,
                   progress = progress)
}

rrq_enqueue_bulk_submit <- function(obj, X, FUN, ..., DOTS = NULL,
                                    do_call = FALSE,
                                    envir = parent.frame(), use_names = TRUE,
                                    key_complete = NULL) {
  ## See queuer:::enqueue_bulk_submit for the general approach used here.
  fun_dat <- queuer::match_fun_queue(FUN, envir, obj$context$envir)
  FUN <- fun_dat$name_symbol %||% fun_dat$value

  ## It is important not to use list(...) here and instead capture the
  ## symbols.  Otherwise later when we print the expression bad things
  ## will happen!
  if (is.null(DOTS)) {
    DOTS <- lapply(lazyeval::lazy_dots(...), "[[", "expr")
  }

  ## TODO: use_names support here not tested
  dat <- context::bulk_prepare_expression(X, FUN, DOTS, do_call, use_names,
                                          envir, obj$db)

  keys <- obj$keys
  key_complete <- key_complete %||% rrq_key_task_complete(keys$queue_name)
  task_dat <- lapply(dat, object_to_bin)
  task_ids <- task_submit_n(obj$con, keys, task_dat, key_complete)
  list(key_complete = key_complete,
       task_ids = task_ids)
}

rrq_enqueue_bulk_wait <- function(obj, dat, timeout = Inf, time_poll = NULL,
                                  progress = NULL) {
  con <- obj$con
  keys <- obj$keys
  task_ids <- dat$task_ids
  key_complete <- dat$key_complete

  ret <- collect_wait_n(con, keys, task_ids, key_complete,
                        timeout = timeout, time_poll = time_poll,
                        progress = progress)
  task_delete(con, keys, task_ids, FALSE)
  setNames(ret, names(task_ids))
}
