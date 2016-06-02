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
  print(try(eval(parse(text=args), .GlobalEnv)))
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

run_message_ENVIR <- function(worker, args) {
  if (worker$initialize_environment(args)) {
    "ENVIR OK"
  } else {
    "ENVIR ERROR"
  }
}

run_message_PAUSE <- function(worker, args) {
  worker$paused <- TRUE
  worker$set_key_queue(clear=TRUE)
  worker$con$HSET(worker$keys$workers_status, worker$name, WORKER_PAUSED)
  "OK"
}

run_message_RESUME <- function(worker, args) {
  worker$paused <- FALSE
  worker$set_key_queue()
  worker$con$HSET(worker$keys$workers_status, worker$name, WORKER_IDLE)
  "OK"
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
