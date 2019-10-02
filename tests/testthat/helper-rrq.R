test_queue_clean <- function(queue_id, delete = TRUE) {
  invisible(rrq_clean(redux::hiredis(), queue_id, delete, "message"))
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

skip_if_no_redis <- function() {
  tryCatch(
    redux::hiredis()$PING(),
    error = function(e) testthat::skip("redis not available"))
  invisible(NULL)
}

wait_status <- function(t, obj, timeout = 2, time_poll = 0.05,
                        status = "PENDING") {
  remaining <- time_checker(timeout)
  while (remaining() > 0) {
    if (all(obj$task_status(t) != status)) {
      return()
    }
    message(".")
    Sys.sleep(time_poll)
  }
  stop(sprintf("Did not change status to %s in time", status))
}


test_hiredis <- function() {
  skip_if_no_redis()
  redux::hiredis()
}


test_rrq <- function(sources = NULL, root = tempfile()) {
  skip_if_no_redis()
  Sys.setenv(R_TESTS = "")

  dir.create(root)
  if (length(sources) > 0) {
    file.copy(sources, root)
    sources <- file.path(root, sources)
  }

  name <- sprintf("rrq:%s", ids::random_id())

  create <- function(envir) {
    for (s in sources) {
      sys.source(s, envir)
    }
  }

  obj <- rrq_controller(name)
  obj$worker_config_save("localhost", time_poll = 1)
  obj$envir(create)
  reg.finalizer(obj, function(e) obj$destroy())
  obj
}


test_worker_spawn <- function(obj, ..., timeout = 10) {
  skip_on_cran()
  worker_spawn(obj, ..., timeout = timeout)
}


test_worker_blocking <- function(obj, worker_config = "localhost", ...) {
  rrq_worker_from_config(obj$queue_id, worker_config, ...)
}


interrupt <- function() {
  structure(list(), class = c("interrupt", "condition"))
}


make_counter <- function(start = 0L) {
  e <- environment()
  e$n <- start
  function() {
    e$n <- e$n + 1L
    e$n
  }
}


with_options <- function(opts, code) {
  oo <- options(opts)
  on.exit(options(oo))
  force(code)
}


options(rrq.progress = FALSE)
