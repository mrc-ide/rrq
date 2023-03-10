## Pulled out because otherwise they clutter the place up.
run_message <- function(worker, private, msg) {
  ## TODO: these can be unserialised...
  content <- bin_to_object(msg)
  message_id <- content$id
  cmd <- content$command
  args <- content$args

  worker$log("MESSAGE", cmd)

  ## TODO: worker restart?  Is that even possible?
  res <- switch(cmd,
                PING = run_message_ping(),
                ECHO = run_message_echo(args),
                EVAL = run_message_eval(args),
                STOP = run_message_stop(worker, private, message_id, args),
                INFO = run_message_info(worker, private),
                PAUSE = run_message_pause(worker, private),
                RESUME = run_message_resume(worker, private),
                REFRESH = run_message_refresh(worker),
                TIMEOUT_SET = run_message_timeout_set(worker, private, args),
                TIMEOUT_GET = run_message_timeout_get(worker, private),
                run_message_unknown(cmd, args))

  response <- message_respond(worker, private, message_id, cmd, res)

  command_resets_timer <- c("PING", "ECHO", "EVAL", "INFO", "PAUSE",
                            "RESUME", "REFRESH")
  if (cmd %in% command_resets_timer) {
    private$timer <- NULL
  }

  response
}

run_message_ping <- function() {
  message("PONG")
  "PONG"
}

run_message_echo <- function(msg) {
  message(msg)
  "OK"
}

run_message_eval <- function(args) {
  if (is.character(args)) {
    args <- parse(text = args)
  }
  print(try(eval(args, .GlobalEnv)))
}

run_message_stop <- function(worker, private, message_id, args) {
  message_respond(worker, private, message_id, "STOP", "BYE")
  if (is.null(args)) {
    args <- "BYE"
  }
  stop(rrq_worker_stop(worker, args))
}

run_message_info <- function(worker, private) {
  info <- worker$info()
  private$con$HSET(private$keys$worker_info, worker$name, object_to_bin(info))
  info
}

run_message_refresh <- function(worker) {
  worker$load_envir()
  "OK"
}

run_message_pause <- function(worker, private) {
  if (private$paused) {
    "NOOP"
  } else {
    private$paused <- TRUE
    private$con$HSET(private$keys$worker_status, worker$name, WORKER_PAUSED)
    "OK"
  }
}

run_message_resume <- function(worker, private) {
  if (private$paused) {
    private$paused <- FALSE
    private$con$HSET(private$keys$worker_status, worker$name, WORKER_IDLE)
    "OK"
  } else {
    "NOOP"
  }
}

run_message_timeout_set <- function(worker, private, args) {
  if (is.numeric(args) || is.null(args)) {
    private$timeout_idle <- args
    worker$timer_start()
    "OK"
  } else {
    "INVALID"
  }
}

run_message_timeout_get <- function(worker, private) {
  if (is.null(private$timeout_idle)) {
    c(timeout_idle = Inf, remaining = Inf)
  } else {
    ## NOTE: This is a slightly odd construction; it arises because
    ## the timer is not just suspended between tasks; it is removed
    ## entirely.  So the worker runs a task (deleting the timer),
    ## and the timer will not be restored until after it makes it
    ## through one BLPOP cycle.  So, if a TIMEOUT_GET message is
    ## issued _immediately_ after running a task then there will be
    ## no timer here.
    if (is.null(private$timer)) {
      remaining <- private$timeout_idle
    } else {
      remaining <- private$timer()
    }
    c(timeout_idle = private$timeout_idle, remaining = remaining)
  }
}

run_message_unknown <- function(cmd, args) {
  msg <- sprintf("Recieved unknown message: [%s]", cmd)
  message(msg)
  structure(list(message = msg, command = cmd, args = args),
            class = c("condition"))
}

message_prepare <- function(id, command, args) {
  object_to_bin(list(id = id, command = command, args = args))
}
response_prepare <- function(id, command, result) {
  object_to_bin(list(id = id, command = command, result = result))
}

message_send <- function(con, keys, command, args = NULL, worker_ids = NULL) {
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  key <- rrq_key_worker_message(keys$queue_id, worker_ids)
  message_id <- redis_time(con)
  content <- message_prepare(message_id, command, args)
  for (k in key) {
    con$RPUSH(k, content)
  }
  invisible(message_id)
}

message_send_and_wait <- function(con, keys, command,
                                  args = NULL, worker_ids = NULL, named = TRUE,
                                  delete = TRUE, timeout = 600,
                                  time_poll = 0.05, progress = NULL) {
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  message_id <- message_send(con, keys, command, args, worker_ids)
  ret <- message_get_response(con, keys, message_id, worker_ids, named, delete,
                              timeout, time_poll, progress)
  if (!delete) {
    attr(ret, "message_id") <- message_id
  }
  ret
}

message_has_response <- function(con, keys, message_id, worker_ids, named) {
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  res <- vnapply(rrq_key_worker_response(keys$queue_id, worker_ids),
                 con$HEXISTS, message_id, USE.NAMES = FALSE)
  res <- as.logical(res)
  if (named) {
    names(res) <- worker_ids
  }
  res
}

message_get_response <- function(con, keys, message_id, worker_ids = NULL,
                                 named = TRUE, delete = FALSE,
                                 timeout = 0, time_poll = 0.05,
                                 progress = NULL) {
  ## NOTE: this won't work well if the message was sent only to a
  ## single worker, or a worker who was not yet started.
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }

  response_keys <- rrq_key_worker_response(keys$queue_id, worker_ids)

  done <- rep(FALSE, length(response_keys))
  fetch <- function() {
    done[!done] <<- hash_exists(con, response_keys[!done], message_id)
    done
  }
  done <- general_poll(fetch, time_poll, timeout, "responses", FALSE, progress)
  if (!all(done)) {
    stop(paste0("Response missing for workers: ",
                paste(worker_ids[!done], collapse = ", ")))
  }

  res <- lapply(response_keys, function(k) {
    bin_to_object(con$HGET(k, message_id))$result
  })

  if (delete) {
    for (k in response_keys) {
      con$HDEL(k, message_id)
    }
  }

  if (named) {
    names(res) <- worker_ids
  }
  res
}


message_response_ids <- function(con, keys, worker_id) {
  response_keys <- rrq_key_worker_response(keys$queue_id, worker_id)
  ids <- as.character(con$HKEYS(response_keys))
  ids[order(as.numeric(ids))]
}


message_respond <- function(worker, private, message_id, cmd, result) {
  worker$log("RESPONSE", cmd)
  response <- response_prepare(message_id, cmd, result)
  private$con$HSET(private$keys$worker_response, message_id, response)
}
