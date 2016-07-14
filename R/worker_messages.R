## Pulled out because otherwise they clutter the place up.
run_message_PING <- function() {
  message("PONG")
  "PONG"
}

run_message_ECHO <- function(msg) {
  message(msg)
  "OK"
}

run_message_EVAL <- function(args) {
  if (is.character(args)) {
    args <- parse(text=args)
  }
  print(try(eval(args, .GlobalEnv)))
}

run_message_STOP <- function(worker, message_id, args) {
  worker$send_response(message_id, "STOP", "BYE")
  if (is.null(args)) {
    args <- "BYE"
  }
  stop(WorkerStop(worker, args))
}

run_message_INFO <- function(worker) {
  info <- worker$print_info()
  worker$con$HSET(worker$keys$workers_info, worker$name,
                  object_to_bin(info))
  info
}

run_message_REFRESH <- function(worker) {
  worker$load_context()
  "OK"
}

run_message_PAUSE <- function(worker) {
  if (worker$paused) {
    "NOOP"
  } else {
    worker$paused <- TRUE
    worker$con$HSET(worker$keys$workers_status, worker$name, WORKER_PAUSED)
    "OK"
  }
}

run_message_RESUME <- function(worker, args) {
  if (worker$paused) {
    worker$paused <- FALSE
    worker$con$HSET(worker$keys$workers_status, worker$name, WORKER_IDLE)
    "OK"
  } else {
    "NOOP"
  }
}

run_message_TIMEOUT_SET <- function(worker, args) {
  if (is.numeric(args) || is.null(args)) {
    worker$timeout <- args
    if (is.null(args)) {
      worker$timer <- NULL
    } else {
      worker$timer <- time_checker(args, remaining=TRUE)
    }
    "OK"
  } else {
    "INVALID"
  }
}

run_message_TIMEOUT_GET <- function(worker) {
  if (is.null(worker$timeout)) {
    c(timeout=Inf, remaining=Inf)
  } else {
    if (is.null(worker$timer)) {
      worker$timer <- time_checker(worker$timeout, TRUE)
    }
    c(timeout=worker$timeout, remaining=worker$timer())
  }
}

run_message_unknown <- function(cmd, args) {
  msg <- sprintf("Recieved unknown message: [%s]", cmd)
  message(msg)
  structure(list(message=msg, command=cmd, args=args),
            class=c("condition"))
}

message_prepare <- function(id, command, args) {
  object_to_bin(list(id=id, command=command, args=args))
}
response_prepare <- function(id, command, result) {
  object_to_bin(list(id=id, command=command, result=result))
}

send_message <- function(con, keys, command, args=NULL, worker_ids=NULL) {
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  key <- rrq_key_worker_message(keys$queue_name, worker_ids)
  message_id <- redis_time(con)
  content <- message_prepare(message_id, command, args)
  for (k in key) {
    con$RPUSH(k, content)
  }
  invisible(message_id)
}

has_responses <- function(con, keys, message_id, worker_ids) {
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  res <- vnapply(rrq_key_worker_response(keys$queue_name, worker_ids),
                 con$HEXISTS, message_id)
  setNames(as.logical(res), worker_ids)
}

has_response <- function(con, keys, message_id, worker_id) {
  assert_scalar(worker_id)
  has_responses(con, keys, message_id, worker_id)[[1L]]
}

get_responses <- function(con, keys, message_id, worker_ids=NULL,
                          delete=FALSE, wait=0, every=0.05) {
  ## NOTE: this won't work well if the message was sent only to a
  ## single worker, or a worker who was not yet started.
  ##
  ## TODO: Could do a progress bar easily enough here.
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  response_keys <- rrq_key_worker_response(keys$queue_name, worker_ids)
  res <- poll_hash_keys(con, response_keys, message_id, wait, every)

  msg <- vlapply(res, is.null)
  if (any(msg)) {
    stop(paste0("Response missing for workers: ",
                paste(worker_ids[msg], collapse=", ")))
  }
  if (delete) {
    for (k in response_keys) {
      con$HDEL(k, message_id)
    }
  }

  names(res) <- worker_ids
  lapply(res, function(x) bin_to_object(x)$result)
}

get_response <- function(con, keys, message_id, worker_id,
                         delete=FALSE, wait=0, every=0.05) {
  assert_scalar(worker_id)
  get_responses(con, keys, message_id, worker_id, delete, wait, every)[[1L]]
}

response_ids <- function(con, keys, worker_id) {
  response_keys <- rrq_key_worker_response(keys$queue_name, worker_id)
  ids <- as.character(con$HKEYS(response_keys))
  ids[order(as.numeric(ids))]
}
