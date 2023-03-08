test_queue_clean <- function(queue_id, delete = TRUE) {
  invisible(rrq_clean(test_hiredis(), queue_id, delete, "message"))
}

has_internet <- function() {
  !is.null(suppressWarnings(utils::nsl("www.google.com")))
}

skip_if_no_internet <- function() {
  skip_on_cran()
  if (has_internet()) {
    return()
  }
  testthat::skip("no internet")
}

skip_if_no_redis <- function() {
  skip_on_cran()
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
    if (!testthat::is_testing()) {
      message(".")
    }
    Sys.sleep(time_poll)
  }
  stop(sprintf("Did not change status from %s in time", status))
}


wait_worker_status <- function(w, obj, status, timeout = 2,
                               time_poll = 0.05) {
  remaining <- time_checker(timeout)
  while (remaining() > 0) {
    if (all(obj$worker_status(w) != status)) {
      return()
    }
    if (!testthat::is_testing()) {
      message(".")
    }
    Sys.sleep(time_poll)
  }
  stop(sprintf("Did not change status from %s in time", status))
}


test_hiredis <- function() {
  skip_if_no_redis()
  redux::hiredis()
}


test_store <- function(..., prefix = NULL) {
  skip_if_not_installed("withr")
  prefix <- prefix %||% sprintf("rrq:test-store:%s", ids::random_id(1, 4))
  con <- test_hiredis()
  st <- object_store$new(con, prefix, ...)
  withr::defer_parent(st$destroy())
  st
}


test_rrq <- function(sources = NULL, root = tempfile(), verbose = FALSE,
                     name = NULL) {
  skip_if_no_redis()
  skip_if_not_installed("withr")
  Sys.setenv(R_TESTS = "")

  dir.create(root)
  if (length(sources) > 0) {
    stopifnot(all(file.copy(sources, root)))
    sources <- file.path(root, sources)
  }

  if (is.null(name)) {
    name <- sprintf("rrq:%s", ids::random_id())
  } else {
    name <- sprintf("rrq:%s:%s", name, ids::random_id())
  }

  create_env <- new.env(parent = globalenv())
  create_env$sources <- sources
  create <- function(envir) {
    for (s in sources) {
      sys.source(s, envir)
    }
  }
  environment(create) <- create_env

  obj <- rrq_controller$new(name)
  obj$worker_config_save("localhost", time_poll = 1, verbose = verbose)
  obj$envir(create)

  withr::defer_parent(test_rrq_cleanup(obj))

  obj
}


test_rrq_cleanup <- function(obj) {
  worker_is_separate <- vnapply(obj$worker_info(), "[[", "pid") != Sys.getpid()
  if (any(worker_is_separate) && !all(worker_is_separate)) {
    warning("This test is likely to leak worker keys", immediate. = TRUE)
  }
  obj$destroy(worker_stop_timeout = if (all(worker_is_separate)) 10 else 0)
}


test_worker_spawn <- function(obj, ..., timeout = 10) {
  skip_on_cran()
  skip_on_windows()
  suppressMessages(rrq_worker_spawn(obj, ..., timeout = timeout))
}


test_worker_blocking <- function(obj, worker_config = "localhost", ...) {
  rrq_worker_from_config(obj$queue_id, worker_config, ...)
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


skip_on_windows <- function() {
  testthat::skip_on_os("windows")
}


r6_private <- function(x) {
  x[[".__enclos_env__"]]$private
}


queue_keys <- function(x) {
  r6_private(x)$keys
}


## Functions change type from function to closure at 4.1, preventing
## use of expect_type
expect_is_function <- function(x) {
  testthat::expect_true(is.function(x))
}


options(rrq.progress = FALSE)
