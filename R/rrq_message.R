##' Send a message to workers. Sending a message returns a message id,
##' which can be used to poll for a response with the other
##' `rrq_message_*` functions.  See `vignette("messages")` for details
##' for the messaging interface.
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
##' @examplesIf rrq::enable_examples(queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' id <- rrq_message_send("PING", controller = obj)
##' rrq_message_get_response(id, timeout = 5, controller = obj)
rrq_message_send <- function(command, args = NULL, worker_ids = NULL,
                             controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  if (is.null(worker_ids)) {
    worker_ids <- rrq_worker_list(controller)
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
##' @examplesIf rrq::enable_examples(queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' id <- rrq_message_send("PING", controller = obj)
##' rrq_message_has_response(id, controller = obj)
##' rrq_message_get_response(id, timeout = 5, controller = obj)
##' rrq_message_has_response(id, controller = obj)
rrq_message_has_response <- function(message_id, worker_ids = NULL,
                                     named = TRUE, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  if (is.null(worker_ids)) {
    worker_ids <- rrq_worker_list(controller)
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
##' @examplesIf rrq::enable_examples(queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##' w <- rrq_worker_list(controller = obj)
##' id <- rrq_message_send("PING", controller = obj)
rrq_message_response_ids <- function(worker_id, controller = NULL) {
  assert_scalar_character(worker_id)
  controller <- get_controller(controller, call = rlang::current_env())
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
##' @examplesIf rrq::enable_examples(queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##' rrq_message_send_and_wait("PING", controller = obj)
rrq_message_send_and_wait <- function(command, args = NULL, worker_ids = NULL,
                                      named = TRUE, delete = TRUE,
                                      timeout = 600, time_poll = 0.05,
                                      progress = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())

  if (is.null(worker_ids)) {
    worker_ids <- rrq_worker_list(controller)
  }
  message_id <- rrq_message_send(command, args, worker_ids, controller)
  ret <- rrq_message_get_response(message_id, worker_ids, named, delete,
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
##' obj <- rrq_controller("rrq:example")
##' id <- rrq_message_send("PING", controller = obj)
##' rrq_message_get_response(id, controller = obj)
rrq_message_get_response <- function(message_id, worker_ids = NULL,
                                     named = TRUE, delete = FALSE, timeout = 0,
                                     time_poll = 0.5, progress = NULL,
                                     controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  ## NOTE: this won't work well if the message was sent only to a
  ## single worker, or a worker who was not yet started.
  if (is.null(worker_ids)) {
    worker_ids <- rrq_worker_list(controller)
  }
  n <- length(worker_ids)

  response_keys <- rrq_key_worker_response(keys$queue_id, worker_ids)

  done <- rep(FALSE, n)
  get_status <- function() {
    done[!done] <- hash_exists(con, response_keys[!done], message_id)
    ifelse(done, "finished", "waiting")
  }

  res <- logwatch::logwatch(
    if (n == 1) "message" else "messages",
    get_status = get_status,
    get_log = NULL,
    show_log = FALSE,
    multiple = n > 1,
    show_spinner = show_progress(progress),
    poll = time_poll,
    timeout = timeout,
    status_waiting = "waiting",
    status_timeout = "wait:timeout",
    status_interrupt = "wait:interrupt")
  is_missing <- res$status %in% c("wait:timeout", "wait:interrupt")
  if (any(is_missing)) {
    msg <- worker_ids[is_missing]
    cli::cli_abort(
      "Response missing for worker{?s}: {squote(msg)}")
  }

  response <- lapply(response_keys, function(k) {
    bin_to_object(con$HGET(k, message_id))$result
  })

  if (delete) {
    for (k in response_keys) {
      con$HDEL(k, message_id)
    }
  }

  if (named) {
    names(response) <- worker_ids
  }
  response
}
