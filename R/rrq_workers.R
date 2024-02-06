rrq_worker_len <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  con$SCARD(keys$worker_id)
}


rrq_worker_list <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  worker_naturalsort(as.character(con$SMEMBERS(keys$worker_id)))
}


rrq_worker_list_exited <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  setdiff(as.character(con$HKEYS(keys$worker_info)),
          rrq_worker_list(controller))
}


rrq_worker_status <- function(worker_ids = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  from_redis_hash(con, keys$worker_status, worker_ids)
}


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


rrq_worker_log_tail <- function(worker_ids = NULL, n = 1, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  ## More intuitive `n` behaviour for "print all entries"; n of Inf
  if (identical(n, Inf)) {
    n <- 0
  }
  # asset_scalar_positive_integer(n)
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
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


rrq_worker_task_id <- function(worker_ids = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  from_redis_hash(con, keys$worker_task, worker_id)
}


rrq_worker_delete_exited <- function(worker_ids = NULL) {
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


rrq_worker_stop <- function(worker_ids = NULL, type = "message",
                            timeout = 0, time_poll = 0.1, progress = NULL,
                            controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  type <- match.arg(type, c("message", "kill", "kill_local"))
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  if (length(worker_ids) == 0L) {
    return(invisible(worker_ids))
  }

  if (type == "message") {
    message_id <- message_send(con, keys, "STOP", worker_ids = worker_ids)
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
    info <- worker_info(con, keys, worker_ids)
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
    info <- worker_info(con, keys, worker_ids)
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


rrq_worker_detect_exited <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  worker_detect_exited(self$con, private$keys, private$store)
}


rrq_worker_process_log <- function(worker_id, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  assert_scalar_character(worker_id)
  path <- self$con$HGET(private$keys$worker_process, worker_id)
  if (is.null(path)) {
    stop("Process log not enabled for this worker")
  }
  readLines(path)
}


rrq_worker_config_save2 <- function(name, config, overwrite = TRUE,
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


rrq_worker_config_list <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  list_to_character(self$con$HKEYS(private$keys$worker_config))
}


rrq_worker_config_read <- function(name, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  worker_config_read(con, keys, name, 0)
}



rrq_worker_load <- function(worker_ids = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())

  ## This is very much a beginning here; it might be nicer to be able
  ## to do this for a given time interval as well as computing a
  ## rolling average (to plot, for example).  But the concept is here
  ## now and we can build off of it.
  logs <- rrq_worker_log_tail(controller, worker_ids, Inf)
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


worker_config_read <- function(con, keys, name, timeout) {
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
