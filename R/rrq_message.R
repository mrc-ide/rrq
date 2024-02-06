##' Send a message to workers. Sending a message returns
##' a message id, which can be used to poll for a response with the
##' other `rrq_message_*` functions.
##'
##' @title Send message to workers
##'
##' @param command A command, such as `PING`, `PAUSE`; see the Messages
##' section of the Details for al messages.
##'
##' @param args Arguments to the command, if supported
##'
##' @param worker_ids Optional vector of worker ids to send the message
##'   to. If `NULL` then the message will be sent to all active workers.
##'
##' @inheritParams rrq_task_list
##'
##' @return Invisibly, a single identifier
##'
##' @export
rrq_message_send <- function(command, args = NULL, worker_ids = NULL,
                             controller = NULL) {
  controller <- get_controller(controller, rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  key <- rrq_key_worker_message(keys$queue_id, worker_ids)
  message_id <- redis_time(con)
  content <- message_prepare(message_id, command, args)
  for (k in key) {
    con$RPUSH(k, content)
  }
  ## TODO: why invisible?
  invisible(message_id)
}


##' Detect if a response is available for a message
##'
##' @title Detect if message has response
##'
##' @param message_id The message id
##'
##' @param worker_ids Optional vector of worker ids. If `NULL` then
##'   all active workers are used (note that this may differ to the set
##'   of workers that the message was sent to!)
##'
##' @param named Logical, indicating if the return vector should be named
##'
##' @inheritParams rrq_task_list
##'
##' @return A logical vector, possibly named (depending on the `named`
##'   argument)
##'
##' @export
rrq_message_has_response <- function(message_id, worker_ids = NULL,
                                     named = TRUE, controller = NULL) {
  controller <- get_controller(controller, rlang::current_env())
  con <- controller$con
  keys <- controller$keys

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


##' Return ids for messages with responses for a particular worker.
##'
##' @param worker_id The worker id
##'
##' @inheritParams rrq_task_list
##'
##' @return A character vector of ids
##'
##' @export
rrq_message_response_ids <- function(worker_id, controller = NULL) {
  controller <- get_controller(controller, rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  response_keys <- rrq_key_worker_response(keys$queue_id, worker_id)
  ids <- as.character(con$HKEYS(response_keys))
  ids[order(as.numeric(ids))]
}


##' Send a message and wait for responses.  This is a helper function
##'   around [rrq_message_send()] and [rrq_message_get_response()].
##'
##' @title Send a message and wait for response
##'
##' @inheritParams rrq_message_send
##' @inheritParams rrq_message_get_response
##'
##' @return The message response
##'
##' @export
rrq_message_send_and_wait <- function(command, args = NULL, worker_ids = NULL,
                                      named = TRUE, delete = TRUE,
                                      timeout = 600, time_poll = 0.05,
                                      progress = NULL, controller = NULL) {
  controller <- get_controller(controller, rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  message_id <- message_send(con, keys, command, args, worker_ids)
  ret <- message_get_response(message_id, worker_ids, named, delete,
                              timeout, time_poll, progress, controller)
  ## TODO: I forget what the logic is here?
  if (!delete) {
    attr(ret, "message_id") <- message_id
  }
  ret
}


##' Get response to messages, waiting until the message has been
##' responded to.
##'
##' @title Get message response
##'
##' @param message_id The message id
##'
##' @param worker_ids Optional vector of worker ids. If `NULL` then
##'   all active workers are used (note that this may differ to the
##'   set of workers that the message was sent to!)
##'
##' @param named Logical, indicating if the return value should be
##'   named by worker id.
##'
##' @param delete Logical, indicating if messages should be deleted
##'   after retrieval
##'
##' @param timeout Integer, representing seconds to wait until the
##'   response has been received. An error will be thrown if a
##'   response has not been received in this time.
##'
##' @param time_poll If `timeout` is greater than zero, this is the
##'   polling interval used between redis calls.  Increasing this
##'   reduces network load but increases the time that may be waited
##'   for.
##'
##' @param progress Optional logical indicating if a progress bar
##'   should be displayed. If `NULL` we fall back on the value of the
##'   global option `rrq.progress`, and if that is unset display a
##'   progress bar if in an interactive session.
##'
##' @inheritParams rrq_task_list
##'
##' @export
rrq_message_get_response <- function(message_id, worker_ids = NULL,
                                     named = TRUE, delete = FALSE, timeout = 0,
                                     time_poll = 0.5, progress = NULL,
                                     controller = NULL) {
  controller <- get_controller(controller, rlang::current_env())
  con <- controller$con
  keys <- controller$keys

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
