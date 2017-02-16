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
rrq_controller <- function(context, con) {
  R6_rrq_controller$new(context, con)
}

R6_rrq_controller <- R6::R6Class(
  "rrq_controller",
  public = list(
    ## TODO: This should either inherit or compose with the worker
    ## controller.
    context = NULL,
    con = NULL,
    keys = NULL,
    envir = NULL,
    db = NULL,

    initialize = function(context, con) {
      assert_inherits(context, "context")
      assert_inherits(con, "redis_api")
      if (!is.environment(context$envir)) {
        stop("context is not loaded")
      }
      self$context <- context
      self$con <- con
      self$keys <- rrq_keys(context$id)
      self$envir <- context$envir
      self$db <- context$db
      write_rrq_worker(self$context)
      ## TODO: I don't know that this is an ideal name, really - I'd
      ## be concerned that something else will clobber this.
      self$worker_config_save("localhost", copy_redis = TRUE, overwrite = FALSE)
      ## This is used to create a hint as to who is using the queue.
      ## It's done as a list so will accumulate elements over time,
      ## but cap at the 10 most recent uses.
      push_controller_info(self$con, self$keys)
    },

    destroy = function(delete = TRUE, type = "message") {
      rrq_clean(self$con, self$context$id, delete, type)
      ## render the controller useless:
      self$context <- NULL
      self$con <- NULL
      self$keys <- NULL
      self$envir <- NULL
      self$db <- NULL
    },

    ## This is like a very stripped down version of queuer's
    ## interface.  I'll deal with locals a bit differently, but
    ## basically not fire them through context in order to save some
    ## time I think.
    ##
    ## In contrast with my other interfaces, for now don't save times.
    ## Keeping it as simple as possible here.
    enqueue = function(expr, envir = parent.frame(), key_complete = NULL) {
      self$enqueue_(substitute(expr), envir, key_complete)
    },

    enqueue_ = function(expr, envir = parent.frame(), key_complete = NULL) {
      dat <- context::prepare_expression(expr, envir, self$db)
      task_submit(self$con, self$keys, dat, key_complete)
    },

    ## TODO: These all need to have similar names, semantics,
    ## arguments as queuer if I will ever merge these and not go mad.
    ## Go through and check.
    tasks_list = function() {
      as.character(self$con$HKEYS(self$keys$tasks_expr))
    },
    tasks_status = function(task_ids = NULL) {
      tasks_status(self$con, self$keys, task_ids)
    },
    task_status = function(task_id) {
      assert_scalar(task_id)
      self$tasks_status(task_id)[[1L]]
    },
    tasks_overview = function(task_ids = NULL) {
      tasks_overview(self$con, self$keys, task_ids)
    },

    ## One result, as the object
    task_result = function(task_id) {
      assert_scalar_character(task_id)
      self$tasks_result(task_id)[[1L]]
    },
    ## zero, one or more tasks as a list
    tasks_result = function(task_ids) {
      task_results(self$con, self$keys, task_ids)
    },

    task_wait = function(task_id, timeout = Inf, time_poll = NULL,
                       progress = FALSE, key_complete = NULL) {
      assert_scalar_character(task_id)
      self$tasks_wait(task_id, timeout, time_poll,
                      progress, key_complete)[[1L]]
    },
    tasks_wait = function(task_ids, timeout = Inf, time_poll = NULL,
                        progress = TRUE, key_complete = NULL) {
      if (is.null(key_complete)) {
        collect_wait_n_poll(self$con, self$keys, task_ids,
                            timeout, time_poll, progress)
      } else {
        collect_wait_n(self$con, self$keys, task_ids, key_complete,
                       timeout, time_poll, progress)
      }
    },
    tasks_delete = function(task_ids, check = TRUE) {
      tasks_delete(self$con, self$keys, task_ids, check)
    },

    queue_length = function() {
      self$con$LLEN(self$keys$queue_rrq)
    },

    queue_list = function() {
      as.character(self$con$LRANGE(self$keys$queue_rrq, 0, -1))
    },

    lapply = function(X, FUN, ..., DOTS = NULL,
                      envir = parent.frame(),
                      timeout = Inf, time_poll = 1, progress = TRUE) {
      rrq_lapply(self, X, FUN, ..., DOTS = NULL, envir = envir,
                 timeout = timeout, time_poll = time_poll,
                 progress = progress)
    },

    enqueue_bulk = function(X, FUN, ..., DOTS = NULL, do_call = FALSE,
                            envir = parent.frame(),
                            timeout = Inf, time_poll = 1, progress = TRUE) {
      rrq_enqueue_bulk(self, X, FUN, ..., DOTS = DOTS, do_call = do_call,
                       envir = envir, timeout = timeout, time_poll = time_poll,
                       progress = progress)
    },

    ## The messaging system from rrqueue, verbatim:
    send_message = function(command, args = NULL, worker_ids = NULL) {
      send_message(self$con, self$keys, command, args, worker_ids)
    },
    has_responses = function(message_id, worker_ids = NULL) {
      has_responses(self$con, self$keys, message_id, worker_ids)
    },
    has_response = function(message_id, worker_id) {
      has_response(self$con, self$keys, message_id, worker_id)
    },
    get_responses = function(message_id, worker_ids = NULL, delete = FALSE,
                             timeout = 0, time_poll = 1, progress = TRUE) {
      get_responses(self$con, self$keys, message_id, worker_ids, delete,
                    timeout, time_poll, progress)
    },
    get_response = function(message_id, worker_id, delete = FALSE,
                            timeout = 0, time_poll = 1, progress = TRUE) {
      get_response(self$con, self$keys, message_id, worker_id, delete,
                   timeout, time_poll, progress)
    },
    response_ids = function(worker_id) {
      response_ids(self$con, self$keys, worker_id)
    },
    send_message_and_wait = function(command, args = NULL, worker_ids = NULL,
                                     delete = TRUE, timeout = 600,
                                     time_poll = 0.05, progress = TRUE) {
      send_message_and_wait(self$con, self$keys, command, args, worker_ids,
                            delete, timeout, time_poll, progress)
    },

    ## Query workers:
    workers_len = function() {
      workers_len(self$con, self$keys)
    },
    workers_list = function() {
      workers_list(self$con, self$keys)
    },
    workers_list_exited = function() {
      workers_list_exited(self$con, self$keys)
    },
    workers_info = function(worker_ids = NULL) {
      workers_info(self$con, self$keys, worker_ids)
    },
    workers_status = function(worker_ids = NULL) {
      workers_status(self$con, self$keys, worker_ids)
    },
    workers_log_tail = function(worker_ids = NULL, n = 1) {
      workers_log_tail(self$con, self$keys, worker_ids, n)
    },
    workers_task_id = function(worker_ids = NULL) {
      workers_task_id(self$con, self$keys, worker_ids)
    },
    workers_delete_exited = function(worker_ids = NULL) {
      workers_delete_exited(self$con, self$keys, worker_ids)
    },

    ## This one is a bit unfortunately named, but should do for now.
    ## It only works if the worker has appropriately saved logging
    ## information.  Given the existance of things like
    ## workers_log_tail this should be renamed something like
    ## worker_text_log perhaps?
    worker_process_log = function(worker_id) {
      assert_scalar(worker_id)
      context::task_log(worker_id, self$context, parse = FALSE)
    },

    workers_stop = function(worker_ids = NULL, type = "message",
                            timeout = 0, time_poll = 1, progress = TRUE) {
      workers_stop(self$con, self$keys, worker_ids, type,
                   timeout, time_poll, progress)
    },

    worker_config_save = function(key, redis_host = NULL, redis_port = NULL,
                                  time_poll = NULL, timeout = NULL,
                                  log_path = NULL,
                                  copy_redis = FALSE, overwrite = TRUE) {
      rrq_worker_config_save(self, key, redis_host, redis_port,
                             time_poll, timeout, log_path, copy_redis, overwrite)
    }
  ))

tasks_status <- function(con, keys, task_ids) {
  from_redis_hash(con, keys$tasks_status, task_ids, missing = TASK_MISSING)
}

tasks_overview <- function(con, keys, task_ids) {
  lvls <- c(TASK_PENDING, TASK_RUNNING, TASK_COMPLETE, TASK_ERROR)
  status <- tasks_status(con, keys, task_ids)
  lvls <- c(lvls, setdiff(unique(status), lvls))
  table(factor(status, lvls))
}

task_submit <- function(con, keys, dat, key_complete) {
  task_submit_n(con, keys, list(object_to_bin(dat)), key_complete)
}

tasks_delete <- function(con, keys, task_ids, check = TRUE) {
  if (check) {
    ## TODO: filter from the running list if not running, but be
    ## aware of race conditions. This is really only for doing
    ## things that have finished so could just check that the
    ## status is one of the finished ones.  Write a small lua
    ## script that can take the setdiff of these perhaps...
    st <- from_redis_hash(con, keys$tasks_status, task_ids,
                          missing = TASK_MISSING)
    if (any(st == "RUNNING")) {
      stop("Can't delete running tasks")
    }
  }
  con$HDEL(keys$tasks_expr,     task_ids)
  con$HDEL(keys$tasks_status,   task_ids)
  con$HDEL(keys$tasks_result,   task_ids)
  con$HDEL(keys$tasks_complete, task_ids)
  con$HDEL(keys$tasks_worker,   task_ids)
}

task_submit_n <- function(con, keys, dat, key_complete) {
  if (!(is.list(dat) && all(vlapply(dat, is.raw)))) {
    stop("dat must be a raw list")
  }
  n <- length(dat)
  task_ids <- ids::random_id(length(dat))

  con$pipeline(
    if (!is.null(key_complete)) {
      redis$HMSET(keys$tasks_complete, task_ids, rep_len(key_complete, n))
    },
    redis$HMSET(keys$tasks_expr, task_ids, dat),
    redis$HMSET(keys$tasks_status, task_ids, rep_len(TASK_PENDING, n)),
    redis$RPUSH(keys$queue_rrq, task_ids))

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

## There are a whole bunch of collection functions here.
##
## * collect_wait_n:      sits on a special key, so is quite responsive
## * collect_wait_n_poll: actively polls the hashes
collect_wait_n <- function(con, keys, task_ids, key_complete,
                           timeout, time_poll, progress) {
  time_poll <- time_poll %||% 1
  assert_integer_like(time_poll)
  if (time_poll < 1L) {
    warning(
      "time_poll cannot be less than 1 with this function; increasing to 1")
    time_poll <- 1L
  }

  status <- from_redis_hash(con, keys$tasks_status, task_ids)

  done <- status == TASK_COMPLETE | status == TASK_ERROR
  if (all(done)) {
    con$DEL(key_complete)
  } else {
    p <- queuer:::progress_timeout(total = length(status),
                                   show = progress, timeout = timeout)
    while (!all(done)) {
      tmp <- con$BLPOP(key_complete, time_poll)
      if (is.null(tmp)) {
        if (p(0)) {
          p(clear = TRUE)
          stop(sprintf("Exceeded maximum time (%d / %d tasks pending)",
                       sum(!done), length(task_ids)))
        }
      } else {
        p(1)
        id <- tmp[[2L]]
        done[[id]] <- TRUE
      }
    }
  }

  task_results(con, keys, task_ids)
}

collect_wait_n_poll <- function(con, keys, task_ids,
                                timeout, time_poll, progress) {
  time_poll <- time_poll %||% 0.1
  status <- from_redis_hash(con, keys$tasks_status, task_ids)
  done <- status == TASK_COMPLETE | status == TASK_ERROR
  if (all(done)) {
    ## pass
  } else if (timeout == 0) {
    stop("Tasks not yet completed; can't be immediately returned")
  } else {
    p <- queuer:::progress_timeout(length(task_ids),
                                   show = progress, timeout = timeout)
    remaining <- task_ids[!done]
    while (length(remaining) > 0L) {
      exists <- viapply(remaining, con$HEXISTS, key = keys$tasks_result) == 1L
      if (any(exists)) {
        i <- remaining[exists]
        p(length(i))
        remaining <- remaining[!exists]
      } else {
        if (p(0)) {
          p(clear = TRUE)
          stop(sprintf("Exceeded maximum time (%d / %d tasks pending)",
                       length(remaining), length(task_ids)))
        }
        Sys.sleep(time_poll)
      }
    }
  }
  task_results(con, keys, task_ids)
}

controller_info <- function() {
  list(hostname = hostname(), pid = process_id(),
       username = username(), time = Sys.time())
}

push_controller_info <- function(con, keys, max_n = 10) {
  n <- con$RPUSH(keys$controllers, object_to_bin(controller_info()))
  if (n > max_n) {
    con$LTRIM(keys$controllers, -max_n, -1)
  }
}
