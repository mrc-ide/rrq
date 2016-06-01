## OK, for what we can do is to ship the environment over, perhaps? Or
## at least make that an option.  The question then becomes how to
## appropriately unpack the environment into the context.

## For now ignore and we'll pick this stuff up later.

## TODO: Register a backend for one of the general parallel packages
## perhaps.

## TODO: I will put the heartbeat stuff back in I think, because we'll
## eventually need to make this fault tolerant.

## TODO: build a queuer compatible interface perhaps, though that
## requires routing everything through context and I'm trying to avoid
## that here I think.

##' A queue controller.  Use this to interact with a queue/cluster.
##' @title rrq queue controller
##'
##' @param context A context handle object.  The context needs to be loaded.
##'
##' @param con A redis connection (redux object).
##'
##' @export
rrq_controller <- function(context, con, envir=.GlobalEnv) {
  .R6_rrq_controller$new(context, con, envir)
}

.R6_rrq_controller <- R6::R6Class(
  "rrq_controller",
  public=list(
    context=NULL,
    con=NULL,
    keys=NULL,
    envir=NULL,
    db=NULL,

    initialize=function(context, con, envir=.GlobalEnv) {
      self$context <- context
      self$con <- con
      self$keys <- rrq_keys(context$id)
      self$envir <- envir
      self$db <- context::context_db(context)
    },

    ## This is like a very stripped down version of queuer's
    ## interface.  I'll deal with locals a bit differently, but
    ## basically not fire them through context in order to save some
    ## time I think.
    ##
    ## In contrast with my other interfaces, for now don't save times.
    ## Keeping it as simple as possible here.
    enqueue=function(expr, envir=parent.frame(), key_complete=NULL) {
      self$enqueue_(substitute(expr), envir, key_complete)
    },

    enqueue_=function(expr, envir=parent.frame(), key_complete=NULL) {
      dat <- prepare_expression(expr, self$envir, self$db)
      task_submit(self$con, self$keys, dat, key_complete)
    },

    ## Collect answer *and* delete tasks.
    collect=function(task_id, delete=TRUE) {
      if (length(task_id) != !L) {
        stop("Expected a scalar task_id")
      }
      collect_n(self$con, self$keys, task_id, delete)[[1L]]
    },

    collect_n=function(task_id, delete=TRUE) {
      collect_n(self$con, self$keys, task_id, delete)
    },

    ## TODO: These all need to have similar names, semantics,
    ## arguments as queuer if I will ever merge these and not go mad.
    ## Go through and check.
    tasks_list=function() {
      as.character(self$con$HKEYS(self$keys$tasks_expr))
    },
    tasks_status=function(task_ids=NULL) {
      tasks_status(self$con, self$keys, task_ids)
    },

    ## One result, as the object
    task_result=function(task_id) {
      assert_scalar_character(task_id)
      self$tasks_result(task_id)[[1L]]
    },
    ## zero, one or more tasks as a list
    tasks_result=function(task_ids) {
      task_results(self$con, self$keys, task_ids)
    },

    task_wait=function(task_id, timeout=Inf, time_poll=0.1,
                       progress_bar=FALSE, key_complete=NULL) {
      assert_scalar_character(task_id)
      self$tasks_wait(task_id, timeout, time_poll,
                      progress_bar, key_complete)[[1L]]
    },
    tasks_wait=function(task_ids, timeout=Inf, time_poll=NULL,
                        progress_bar=TRUE, key_complete=NULL) {
      if (is.null(key_complete)) {
        collect_wait_n_poll(self$con, self$keys, task_ids,
                            timeout, time_poll, progress_bar)
      } else {
        collect_wait_n(self$con, self$keys, task_ids, key_complete,
                       timeout, time_poll, progress_bar)
      }
    },
    tasks_delete=function(task_ids, check=TRUE) {
      tasks_delete(self$con, self$keys, task_ids, check)
    },

    queue_length=function() {
      self$con$LLEN(self$keys$tasks_queue)
    },

    queue_list=function() {
      as.character(self$con$LRANGE(self$keys$tasks_queue, 0, -1))
    },


    lapply=function(X, FUN, ..., envir=parent.frame(),
                    timeout=Inf, time_poll=1, progress_bar=TRUE) {
      rrq_lapply(self$con, self$keys, X, FUN, ...,
                 envir=envir, queue_envir=self$envir,
                 timeout=timeout, time_poll=time_poll,
                 progress_bar=progress_bar)
    },

    ## The messaging system from rrqueue, verbatim:
    send_message=function(command, args=NULL, worker_ids=NULL) {
      send_message(self$con, self$keys, command, args, worker_ids)
    },
    has_responses=function(message_id, worker_ids=NULL) {
      has_responses(self$con, self$keys, message_id, worker_ids)
    },
    get_responses=function(message_id, worker_ids=NULL, delete=FALSE, wait=0) {
      get_responses(self$con, self$keys, message_id, worker_ids, delete, wait)
    },
    get_response=function(message_id, worker_id, delete=FALSE, wait=0) {
      self$get_responses(message_id, worker_id, delete, wait)[[1L]]
    },
    response_ids=function(worker_id) {
      response_keys <- rrq_key_worker_response(self$context$id, worker_id)
      ids <- as.character(self$con$HKEYS(response_keys))
      ids[order(as.numeric(ids))]
    },
    workers_list=function() {
      workers_list(self$con, self$keys)
    }

    ## But the most common thing is going to be to run a bunch of jobs
    ## in a row with relatively low latency.  That's going to involve
    ## writing a bunch of data, bunch of expressions and then
    ## evaluating them.
    ))

tasks_status <- function(con, keys, task_id) {
  from_redis_hash(con, keys$tasks_status, task_id, missing=TASK_MISSING)
}

task_submit <- function(con, keys, dat, key_complete) {
  task_submit_n(con, keys, list(object_to_bin(dat)), key_complete)
}

tasks_delete <- function(con, keys, task_ids, check=TRUE) {
  if (check) {
    ## TODO: filter from the running list if not running, but be
    ## aware of race conditions. This is really only for doing
    ## things that have finished so could just check that the
    ## status is one of the finished ones.  Write a small lua
    ## script that can take the setdiff of these perhaps...
    st <- from_redis_hash(con, keys$tasks_status, task_id,
                          missing=TASK_MISSING)
    if (any(st == "RUNNING")) {
      stop("Can't delete running tasks")
    }
  }
  con$HDEL(keys$tasks_expr,     task_id)
  con$HDEL(keys$tasks_status,   task_id)
  con$HDEL(keys$tasks_result,   task_id)
  con$HDEL(keys$tasks_complete, task_id)
  con$HDEL(keys$tasks_worker,   task_id)
}

task_submit_n <- function(con, keys, dat, key_complete) {
  if (!(is.list(dat) && all(vlapply(dat, is.raw)))) {
    stop("dat must be a raw list")
  }
  n <- length(dat)
  task_ids <- ids::random_id(length(dat))

  if (!is.null(key_complete)) {
    con$HMSET(keys$tasks_complete, task_ids, rep_len(key_complete, n))
  }
  con$HMSET(keys$tasks_expr, task_ids, dat)
  con$HMSET(keys$tasks_status, task_ids, rep_len(TASK_PENDING, n))
  ## Must be last:
  con$RPUSH(keys$tasks_queue, task_ids)

  task_ids
}

task_results <- function(con, keys, task_ids) {
  res <- from_redis_hash(con, keys$tasks_result, task_ids, identity, NULL)
  err <- lengths(res) == 0L
  if (any(err)) {
    stop("Missing some results")
  }
  lapply(res, bin_to_object)
}

## NOTE: this is largely duplicated from the queuer::qlapply but
## tweaked a bit.  I'll be rolling these together perhaps at some
## point.
##
## The difference is that we go through and prepare the *template*
## once (including copying locals around), then go through and queue
## the prepared expressions.  This is a good approach to use in queuer
## too.  The workers will do a reasonable job of not reloading locals.
rrq_lapply <- function(con, keys, X, FUN, ..., envir, queue_envir,
                       timeout=Inf, time_poll=NULL, progress_bar=TRUE) {
  XX <- as.list(X)
  n <- length(XX)
  DOTS <- lapply(lazyeval::lazy_dots(...), "[[", "expr")
  key_complete <- ids::random_id()

  fun <- queuer::find_fun_queue(FUN, envir, queue_envir)
  template <- as.call(c(list(fun), list(NULL), DOTS))
  dat <- prepare_expression(template, self$envir, self$db)
  f <- function(x) {
    dat$expr[[2L]] <- x
    object_to_bin(dat)
  }
  task_ids <- task_submit_n(con, keys, lapply(XX, f), key_complete)

  ## TODO: Given that this will throw and the ids will be lost
  ## forever, it probably makes sense to try and flush the queue at
  ## this point, or implement various error handling approaches.
  ret <- collect_wait_n(con, keys, task_ids, key_complete,
                        timeout=timeout, time_poll=time_poll,
                        progress_bar=progress_bar)
  setNames(ret, names(X))
}

## There are a whole bunch of collection functions here.
##
## * collect_wait_n:      sits on a special key, so is quite responsive
## * collect_wait_n_poll: actively polls the hashes
## * collect_n:           collects straight away, but also deletes!
collect_wait_n <- function(con, keys, task_ids, key_complete,
                           timeout=Inf, time_poll=NULL, progress_bar=TRUE) {
  time_poll <- time_poll %||% 1
  status <- from_redis_hash(con, keys$tasks_status, task_ids)
  done <- status == TASK_COMPLETE | status == TASK_ERROR
  res <- setNames(vector("list", length(task_ids)), task_ids)
  if (any(done)) {
    res[done] <- task_results(con, keys, task_ids[done])
  }

  times_up <- time_checker(timeout)
  p <- progress(length(task_ids), show=progress_bar)
  assert_integer_like(time_poll)

  ## Or poll:
  while (!all(done)) {
    if (times_up()) {
      if (progress_bar) {
        message()
      }
      stop(sprintf("Exceeded maximum time (%d / %d tasks pending)",
                   sum(!done), length(task_ids)))
    }
    tmp <- con$BLPOP(key_complete, time_poll)
    if (is.null(tmp)) {
      p(0)
    } else {
      p(1)
      id <- tmp[[2L]]
      if (!done[[id]]) {
        res[id] <- task_results(con, keys, id)
        done[[id]] <- TRUE
      }
    }
  }

  res
}

collect_wait_n_poll <- function(con, keys, task_ids, timeout, time_poll,
                                progress_bar=TRUE) {
  time_poll <- time_poll %||% 0.1
  status <- from_redis_hash(con, keys$tasks_status, task_ids)
  done <- status == TASK_COMPLETE | status == TASK_ERROR
  res <- setNames(vector("list", length(task_ids)), task_ids)
  if (any(done)) {
    res[done] <- task_results(con, keys, task_ids[done])
  }
  if (all(done)) {
    return(res)
  } else if (timeout == 0) {
    stop("Tasks not yet completed; can't be immediately returned")
  }

  times_up <- time_checker(timeout)
  p <- progress(length(task_ids), show=progress_bar)
  remaining <- task_ids[!done]

  ## Or poll:
  while (length(remaining) > 0L) {
    if (times_up()) {
      if (progress_bar) {
        message()
      }
      stop(sprintf("Exceeded maximum time (%d / %d tasks pending)",
                   length(remaining), length(task_ids)))
    }

    ok <- viapply(remaining, con$HEXISTS, key=keys$tasks_result) == 1L
    if (any(ok)) {
      i <- remaining[ok]
      p(length(i))
      res[i] <- task_results(con, keys, i)
      remaining <- remaining[!ok]
    } else {
      p(0)
      Sys.sleep(time_poll)
    }
  }

  res
}

collect_n <- function(con, keys, task_id, delete) {
  status <- tasks_status(con, keys, task_id)
  ok <- status == TASK_COMPLETE | status == TASK_ERROR
  if (all(ok)) {
    res <- task_results(con, keys, task_id)
    if (delete) {
      self$task_delete(task_id, FALSE)
    }
  } else {
    stop(sprintf("tasks %s is unfetchable: %s",
                 paste(task_id[ok], collapse=", "),
                 paste(status[ok], collapse=", ")))
  }
  res
}
