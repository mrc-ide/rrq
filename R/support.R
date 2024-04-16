rrq_clean <- function(controller, delete = 0, worker_stop = FALSE,
                      timeout_worker_stop = 0) {
  if (!identical(worker_stop, FALSE)) {
    type <- if (isTRUE(worker_stop)) "message" else worker_stop
    rrq_worker_stop(type = type, timeout = timeout_worker_stop,
                    progress = FALSE, controller = controller)
  }
  pat <- sprintf("%s:*", queue_id)
  delete_keys(con, pat, delete)
}
