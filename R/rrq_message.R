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
  invisible(message_id)
}

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


rrq_message_response_ids <- function(worker_id, controller = NULL) {
  controller <- get_controller(controller, rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  response_keys <- rrq_key_worker_response(keys$queue_id, worker_id)
  ids <- as.character(con$HKEYS(response_keys))
  ids[order(as.numeric(ids))]
}


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
  if (!delete) {
    attr(ret, "message_id") <- message_id
  }
  ret
}


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
                                     
