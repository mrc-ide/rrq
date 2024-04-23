##' Returns the number of active workers
##'
##' @title Number of active workers
##'
##' @inheritParams rrq_task_list
##'
##' @return An integer
##'
##' @export
rrq_worker_len <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  con$SCARD(keys$worker_id)
}


##' Returns the ids of active workers.  This does not include exited
##' workers; use [rrq_worker_list_exited()] for that.
##'
##' @title List active workers
##'
##' @inheritParams rrq_task_list
##'
##' @return A character vector of worker names
##'
##' @export
rrq_worker_list <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  worker_naturalsort(as.character(con$SMEMBERS(keys$worker_id)))
}


##' Test if a worker exists
##'
##' @title Test if a worker exists
##'
##' @param name Name of the worker
##'
##' @inheritParams rrq_task_list
##'
##' @return A logical value
##' @export
rrq_worker_exists <- function(name, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_scalar_character(name, call = environment())
  con <- controller$con
  keys <- controller$keys
  con$SISMEMBER(keys$worker_id, name) == 1L
}


##' Returns the ids of workers known to have exited
##'
##' @title List exited workers
##'
##' @inheritParams rrq_task_list
##'
##' @return A character vector of worker names
##'
##' @export
rrq_worker_list_exited <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  setdiff(as.character(con$HKEYS(keys$worker_info)),
          rrq_worker_list(controller))
}


##' Returns a character vector of current worker statuses
##'
##' @title Worker statuses
##'
##' @param worker_ids Optional vector of worker ids. If `NULL` then
##'   all active workers are used.
##'
##' @inheritParams rrq_task_list
##'
##' @return A character vector of statuses, named by worker
##'
##' @export
rrq_worker_status <- function(worker_ids = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  from_redis_hash(con, keys$worker_status, worker_ids)
}


##' Returns a list of information about active workers (or exited
##' workers if `worker_ids` includes them).
##'
##' @title Worker information
##
##' @param worker_ids Optional vector of worker ids. If `NULL` then
##' all active workers are used.
##'
##' @inheritParams rrq_task_list
##'
##' @return A list of `worker_info` objects
##'
##' @export
rrq_worker_info <- function(worker_ids = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  ret <- from_redis_hash(con, keys$worker_info, worker_ids,
                         f = Vectorize(bin_to_object_safe, SIMPLIFY = FALSE))
  lapply(ret, function(x) {
    class(x) <- "rrq_worker_info"
    x
  })
}


##' Returns the last (few) elements in the worker log, in a
##' programmatically useful format (see Value).
##'
##' @param worker_ids Optional vector of worker ids. If `NULL` then
##' all active workers are used.
##'
##' @param n Number of elements to select, the default being the single
##' last entry. Use `Inf` or `0` to indicate that you want all log entries
##'
##' @return A [data.frame] with columns:
##'
##' * `worker_id`: the worker id
##' * `child`: the process id, an integer, where logs come from a child
##'   process from a task queued with `separate_process = TRUE`
##' * `time`: the time from Redis when the event happened; see
##'   [redux::redis_time] to convert this to an R time
##' * `command`: the command sent from or to the worker
##' * `message`: the message corresponding to that command
##'
##' @inheritParams rrq_task_list
##'
##' @export
rrq_worker_log_tail <- function(worker_ids = NULL, n = 1, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  ## More intuitive `n` behaviour for "print all entries"; n of Inf
  if (identical(n, Inf)) {
    n <- 0
  }
  if (is.null(worker_ids)) {
    worker_ids <- rrq_worker_list(controller)
  } else {
    assert_character(worker_ids)
  }

  logs <- lapply(worker_ids, function(i) worker_log_tail_1(con, keys, i, n))
  if (length(logs) > 0L) {
    n <- viapply(logs, nrow)
    ret <- do.call("rbind", logs, quote = TRUE)
    ret <- ret[order(ret$time, ret$worker_id), ]
    rownames(ret) <- NULL
    ret
  } else {
    data_frame(worker_id = character(0),
               child = integer(0),
               time = numeric(0),
               command = character(0),
               message = character(0))
  }
}


##' Returns the task id that each worker is working on, if any.
##'
##' @title Current task id for workers
##'
##' @param worker_ids Optional vector of worker ids. If `NULL` then
##' all active workers are used.
##'
##' @inheritParams rrq_task_list
##'
##' @return A character vector, `NA` where nothing is being worked on,
##'   otherwise corresponding to a task id.
##'
##' @export
rrq_worker_task_id <- function(worker_ids = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  from_redis_hash(con, keys$worker_task, worker_ids)
}


##' Cleans up workers known to have exited
##'
##' @title Clean up exited workers
##'
##' @param worker_ids Optional vector of worker ids. If `NULL` then
##'   rrq looks for exited workers using [rrq_worker_list_exited()].
##'   If given, we check that the workers are known and have exited.
##'
##' @inheritParams rrq_task_list
##'
##' @return A character vector of workers that were deleted
##'
##' @export
rrq_worker_delete_exited <- function(worker_ids = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  exited <- rrq_worker_list_exited(controller)
  if (is.null(worker_ids)) {
    worker_ids <- exited
  }
  extra <- setdiff(worker_ids, exited)
  if (length(extra)) {
    ## TODO: this whole thing can be improved because we might want to
    ## inform the user if the workers are not known.
    stop(sprintf("Workers %s may not have exited or may not exist",
                 paste(extra, collapse = ", ")))
  }

  if (length(worker_ids) > 0L) {
    con$SREM(keys$worker_id,     worker_ids)
    con$HDEL(keys$worker_status, worker_ids)
    con$HDEL(keys$worker_task,   worker_ids)
    con$HDEL(keys$worker_info,   worker_ids)
    con$DEL(c(rrq_key_worker_log(keys$queue_id, worker_ids),
              rrq_key_worker_message(keys$queue_id, worker_ids),
              rrq_key_worker_response(keys$queue_id, worker_ids)))
  }
  worker_ids
}


##' Stop workers.
##'
##' The `type` parameter indicates the strategy used to stop workers,
##' and interacts with other parameters. The strategies used by the
##' different values are:
##'
##' * `message`, in which case a `STOP` message will be sent to the
##'   worker, which they will receive after finishing any currently
##'   running task (if `RUNNING`; `IDLE` workers will stop immediately).
##' * `kill`, in which case a kill signal will be sent via the heartbeat
##'   (if the worker is using one). This will kill the worker even if
##'   is currently working on a task, eventually leaving that task with
##'   a status of `DIED`.
##' * `kill_local`, in which case a kill signal is sent using operating
##'    system signals, which requires that the worker is on the same
##'    machine as the controller.
##'
##' @title Stop workers
##'
##' @param worker_ids Optional vector of worker ids. If `NULL` then
##' all active workers will be stopped.
##'
##' @param type The strategy used to stop the workers. Can be `message`,
##'   `kill` or `kill_local` (see Details).
##'
##' @param timeout Optional timeout; if greater than zero then we poll
##'   for a response from the worker for this many seconds until they
##'   acknowledge the message and stop (only has an effect if `type`
##'   is `message`). If a timeout of greater than zero is given, then
##'   for a `message`-based stop we wait up to this many seconds for the
##'   worker to exit. That means that we might wait up to `2 * timeout`
##'   seconds for this function to return.
##'
##' @param time_poll If `type` is `message` and `timeout` is greater
##'   than zero, this is the polling interval used between redis calls.
##'   Increasing this reduces network load but decreases the ability
##'   to interrupt the process.
##'
##' @param progress Optional logical indicating if a progress bar
##'   should be displayed. If `NULL` we fall back on the value of the
##'   global option `rrq.progress`, and if that is unset display a
##'   progress bar if in an interactive session.
##'
##' @inheritParams rrq_task_list
##'
##' @return The names of the stopped workers, invisibly.
##'
##' @export
rrq_worker_stop <- function(worker_ids = NULL, type = "message",
                            timeout = 0, time_poll = 0.1, progress = NULL,
                            controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  type <- match.arg(type, c("message", "kill", "kill_local"))
  if (is.null(worker_ids)) {
    worker_ids <- rrq_worker_list(controller)
  }
  if (length(worker_ids) == 0L) {
    return(invisible(worker_ids))
  }

  if (type == "message") {
    message_id <- rrq_message_send("STOP", worker_ids = worker_ids,
                                   controller = controller)
    if (timeout > 0L) {
      rrq_message_get_response(message_id, worker_ids, delete = FALSE,
                               timeout = timeout, time_poll = time_poll,
                               progress = progress, controller = controller)
      key_status <- keys$worker_status
      when <- function() {
        any(list_to_character(con$HMGET(key_status, worker_ids)) != "EXITED")
      }
      wait_timeout("Worker did not exit in time", timeout, when, time_poll)
    }
  } else if (type == "kill") {
    info <- rrq_worker_info(worker_ids, controller = controller)
    heartbeat_key <- vcapply(info, function(x) {
      x$heartbeat_key %||% NA_character_
    })
    if (any(is.na(heartbeat_key))) {
      stop("Worker does not support heatbeat - can't kill with signal: ",
           paste(worker_ids[is.na(heartbeat_key)], collapse = ", "))
    }
    for (key in heartbeat_key) {
      rrq_heartbeat_kill(con, key, tools::SIGTERM)
    }
  } else { # kill_local
    info <- rrq_worker_info(worker_ids, controller = controller)
    is_local <- vcapply(info, "[[", "hostname") == hostname()
    if (!all(is_local)) {
      stop("Not all workers are local: ",
           paste(worker_ids[!is_local], collapse = ", "))
    }
    ## It might be possible to check to see if the process is alive -
    ## that's easiest done with the ps package perhaps, but I think
    ## there's a (somewhat portable) way of doing it with base R.
    tools::pskill(vnapply(info, "[[", "pid"), tools::SIGTERM)
  }

  invisible(worker_ids)
}


##' Detects exited workers through a lapsed heartbeat.  This differs
##' from [rrq_worker_list_exited()] which lists workers that have
##' definitely exited by checking to see if any worker that runs a
##' heartbeat process has not reported back in time, then marks that
##' worker as exited.  See vignette("fault-tolerance") for details.
##'
##' @title Detect exited workers
##'
##' @inheritParams rrq_task_list
##'
##' @export
rrq_worker_detect_exited <- function(controller = NULL) {
  ## TODO: should accept a worker_ids argument I think
  controller <- get_controller(controller, call = rlang::current_env())
  time <- heartbeat_time_remaining(controller)
  cleanup_orphans(controller, time)
}


##' Return the contents of a worker's process log, if
##' it is located on the same physical storage (including network
##' storage) as the controller. This will generally behave for
##' workers started with [rrq_worker_spawn] but may require significant
##' care otherwise.
##'
##' @title Read worker process log
##'
##' @param worker_id The worker id for which the log is required
##'
##' @inheritParams rrq_task_list
##' @export
rrq_worker_process_log <- function(worker_id, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  assert_scalar_character(worker_id)
  path <- con$HGET(keys$worker_process, worker_id)
  if (is.null(path)) {
    stop("Process log not enabled for this worker")
  }
  readLines(path)
}


##' Register a function to create an environment when creating a
##'   worker. When a worker starts, they will run this function.
##'
##' @title Set worker environment
##'
##' @param create A function that will create an environment. It will
##'   be called with one parameter (an environment), in a fresh R
##'   session. The function [rrq::rrq_envir()] can be used to
##'   create a suitable function for the most common case (loading
##'   packages and sourcing scripts).
##'
##' @param notify Boolean, indicating if we should send a `REFRESH`
##'   message to all workers to update their environment.
##'
##' @inheritParams rrq_task_list
##'
##' @export
rrq_worker_envir_set <- function(create, notify = TRUE, controller = NULL) {
  controller <- get_controller(controller)
  con <- controller$con
  keys <- controller$keys

  if (is.null(create)) {
    con$DEL(keys$envir)
  } else {
    assert_is(create, "function")
    con$SET(keys$envir, object_to_bin(create))
  }
  if (notify) {
    rrq_message_send("REFRESH", controller = controller)
  }
}


##' Save a worker configuration, which can be used to start workers
##' with a set of options with the cli. These correspond to arguments
##' to [rrq::rrq_worker]. **This function will be renamed soon**
##'
##' @title Save worker configuration
##'
##' @param name Name for this configuration
##'
##' @param config A worker configuration, created by
##'   [rrq::rrq_worker_config()]
##'
##' @param overwrite Logical, indicating if an existing configuration
##'   with this `name` should be overwritten if it exists. If `FALSE`,
##'   then the configuration is not updated, even if it differs from
##'   the version currently saved.
##'
##' @inheritParams rrq_task_list
##'
##' @return Invisibly, a boolean indicating if the configuration was
##'   updated.
##'
##' @export
rrq_worker_config_save <- function(name, config, overwrite = TRUE,
                                   controller = NULL) {
  ## TODO: odd name here while we transition to new interface, clashes
  ## with rrq_worker_config_save, used in hint.
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  assert_is(config, "rrq_worker_config")
  write <- overwrite || con$HEXISTS(keys$worker_config, name) == 0
  if (write) {
    con$HSET(keys$worker_config, name, object_to_bin(config))
  }
  invisible(write)
}



##' Return names of worker configurations saved by
##' [rrq_worker_config_save()]
##'
##' @title List worker configurations
##'
##' @inheritParams rrq_task_list
##'
##' @return A character vector of names; these can be passed as the
##'   `name` argument to [rrq_worker_config_read()].
##'
##' @export
rrq_worker_config_list <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  list_to_character(con$HKEYS(keys$worker_config))
}


##' Return the value of a of worker configuration saved by
##'   [rrq_worker_config_save()]
##'
##' @title Read worker configuration
##'
##' @param name Name of the configuration (see
##'   [rrq_worker_config_list()])
##'
##' @param timeout Optionally, a timeout to wait for a worker
##'   configuration to appear.  Generally you won't want to set this,
##'   but it can be used to block until a configuration becomes
##'   available.
##'
##' @inheritParams rrq_task_list
##'
##' @export
rrq_worker_config_read <- function(name, timeout = 0, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  read <- function() {
    config <- con$HGET(keys$worker_config, name)
    if (is.null(config)) {
      cli::cli_abort("Invalid rrq worker configuration key '{name}'")
    }
    bin_to_object(config)
  }

  if (timeout > 0) {
    time_poll <- 1
    wait_success("config not readable in time", timeout, read, time_poll)
  } else {
    read()
  }
}


##' Report on worker "load" (the number of workers being used over
##' time). Reruns an object of class `worker_load`, for which a
##' `mean` method exists (this function is a work in progress and the
##' interface may change).
##'
##' @title Report on worker load
##'
##' @param worker_ids Optional vector of worker ids. If `NULL` then
##'   all active workers are used.
##'
##' @inheritParams rrq_task_list
##'
##' @return An object of class "worker_load", which has a pretty print
##'   method.
##'
##' @export
rrq_worker_load <- function(worker_ids = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())

  ## This is very much a beginning here; it might be nicer to be able
  ## to do this for a given time interval as well as computing a
  ## rolling average (to plot, for example).  But the concept is here
  ## now and we can build off of it.
  logs <- rrq_worker_log_tail(worker_ids, Inf, controller)
  logs <- logs[order(logs$time), ]

  keep <- c("ALIVE", "STOP", "TASK_START", "TASK_COMPLETE")
  logs <- logs[logs$command %in% keep & is.na(logs$child), ]

  logs$worker <- 0
  logs$worker[logs$command == "ALIVE"] <- 1
  logs$worker[logs$command == "STOP"] <- -1
  logs$worker_cum <- cumsum(logs$worker)

  logs$task <- 0
  logs$task[logs$command == "TASK_START"] <- 1
  logs$task[logs$command == "TASK_COMPLETE"] <- -1
  logs$task_cum <- cumsum(logs$task)

  logs$dt <- c(diff(logs$time), 0)
  logs$ago <- logs$time[nrow(logs)] - logs$time

  class(logs) <- c("worker_load", class(logs))
  logs
}


## TODO: better to return a character vector and destructure later
worker_log_tail_1 <- function(con, keys, worker_id, n = 1) {
  log_key <- rrq_key_worker_log(keys$queue_id, worker_id)
  log <- as.character(con$LRANGE(log_key, -n, -1))
  worker_log_parse(log, worker_id)
}


worker_log_parse <- function(log, worker_id) {
  re <- "^([0-9.]+)(/[0-9]+)? ([^ ]+) ?(.*)$"
  if (!all(grepl(re, log))) {
    stop("Corrupt log")
  }
  time <- as.numeric(sub(re, "\\1", log))
  child <- as.integer(sub("/", "", sub(re, "\\2", log)))
  command <- sub(re, "\\3", log)
  message <- lstrip(sub(re, "\\4", log))
  data_frame(worker_id, child, time, command, message)
}


##' @export
mean.worker_load <- function(x, time = c(1, 5, 15, Inf), ...) {
  ## make this slightly nicer to work with:
  x$dt <- x$dt * 1000
  x$ago <- x$ago * 1000

  f <- function(t) {
    i <- which(x$ago < t)[[1L]] - 1L
    y <- x[i:nrow(x), ]
    if (i > 0) {
      dt <- y$ago[[1L]] - t
      y$dt[[1L]] <- y$dt[[1L]] - dt
      y$time[[1L]] <- y$time[[1L]] - dt
      y$ago[[1L]] <- t
    }
    c(used = sum(y$task_cum * y$dt) / y$ago[[1L]],
      available = sum(y$worker_cum * y$dt) / y$ago[[1L]])
  }
  res <- vapply(time, f, numeric(2))
  colnames(res) <- as.character(time)
  res
}
