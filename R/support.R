workers_info <- function(con, keys, worker_ids=NULL) {
  from_redis_hash(con, keys$workers_info, worker_ids,
                  f=Vectorize(bin_to_object, SIMPLIFY=FALSE))
}

stop_workers <- function(con, keys, worker_ids=NULL, type="message", wait=0) {
  type <- match.arg(type, c("message", "kill", "kill_local"))
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  if (length(worker_ids) == 0L) {
    return(invisible(worker_ids))
  }

  if (type == "message") {
    ## First, we send a message saying 'STOP'
    message_id <- send_message(con, keys, "STOP", worker_ids=worker_ids)
    ## ## TODO: This needs RedisHeartbeat support
    ##
    ## if (interrupt) {
    ##   is_busy <- workers_status(con, keys, worker_ids) == WORKER_BUSY
    ##   if (any(is_busy)) {
    ##     queue_send_signal(con, keys, tools::SIGINT, worker_ids[is_busy])
    ##   }
    ## }
    if (wait > 0L) {
      ok <- try(get_responses(con, keys, message_id, worker_ids,
                              delete=FALSE, wait=wait),
                silent=TRUE)
      ## if (is_error(ok)) {
      ##   done <- has_responses(con, keys, message_id, worker_ids)
      ##   stop_workers(con, keys, worker_ids[!done], "kill")
      ## }
    }
    ## TODO: This requires RedisHeartbeat support
  } else if (type == "kill") {
    ## queue_send_signal(con, keys, tools::SIGTERM, worker_ids)
    stop("Needs redis heartbeat support")
  } else { # kill_local
    w_info <- workers_info(con, keys, worker_ids)
    w_local <- vcapply(w_info, "[[", "hostname") == hostname()
    if (!all(w_local)) {
      stop("Not all workers are local: ",
           paste(worker_ids[!w_local], collapse=", "))
    }
    tools::pskill(vnapply(w_info, "[[", "pid"), tools::SIGTERM)
  }
  invisible(worker_ids)
}

rrq_clean <- function(con, queue_name, delete=0, stop_workers=FALSE) {
  keys <- rrq_keys(queue_name)
  if (!identical(stop_workers, FALSE)) {
    type <- if (isTRUE(stop_workers)) "message" else stop_workers
    stop_workers(con, keys, type=type, wait=.1)
  }
  pat <- sprintf("rrq:%s:*", queue_name)
  if (isTRUE(delete) || delete == 0) {
    redux::scan_del(con, pat)
  } else if (delete > 0) {
    scan_expire(con, pat, delete)
  } else {
    stop("Invalid value for delete")
  }
}
