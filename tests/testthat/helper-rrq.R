test_queue_clean <- function(context_id, delete=TRUE) {
  invisible(rrq_clean(redux::hiredis(), context_id, delete, "message"))
}

temp_context <- function(sources=NULL, ...) {
  root <- tempfile()
  dir.create(root, TRUE, FALSE)
  if (length(sources) > 0L) {
    file.copy(sources, root)
  }
  context::context_save(root, sources=sources, ...)
}

worker_command <- function(obj) {
  root <- obj$context$root$path
  context_id <- obj$context$id
  bquote(rrq_worker_from_config(.(root), .(context_id), "localhost"))
}

has_internet <- function() {
  !is.null(suppressWarnings(utils::nsl("www.google.com")))
}

skip_if_no_internet <- function() {
  if (has_internet()) {
    return()
  }
  testthat::skip("no internet")
}

wait_status <- function(t, obj, timeout = 2, time_poll = 0.05,
                        status = "PENDING") {
  times_up <- queuer:::time_checker(timeout)
  while (!times_up()) {
    if (all(obj$task_status(t) != status)) {
      return()
    }
    message(".")
    Sys.sleep(time_poll)
  }
  stop(sprintf("Did not change status to %s in time", status))
}

PROGRESS <- FALSE
