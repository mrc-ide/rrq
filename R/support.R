rrq_clean <- function(con, queue_id, delete = 0, worker_stop = FALSE,
                      timeout_worker_stop = 0) {
  controller <- rrq_controller(queue_id, con, check_version = FALSE)
  if (!identical(worker_stop, FALSE)) {
    type <- if (isTRUE(worker_stop)) "message" else worker_stop
    rrq_worker_stop(type = type, timeout = timeout_worker_stop,
                    progress = FALSE, controller = controller)
  }
  pat <- sprintf("%s:*", queue_id)
  delete_keys(con, pat, delete)
}
