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
  testthat::skip_if_not(redis_available(), "redis not available")
}

wait_status <- function(t, obj, timeout = 2, time_poll = 0.05,
                        status = "PENDING") {
  remaining <- time_checker(timeout)
  while (remaining() > 0) {
    if (all(rrq_task_status(t, controller = obj) != status)) {
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
    if (all(rrq_worker_status(w, controller = obj) != status)) {
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


test_name <- function(name = NULL) {
  sprintf("%s:%s", name %||% "rrq", ids::random_id())
}


test_rrq <- function(sources = NULL, root = tempfile(), verbose = FALSE,
                     name = NULL, timeout_worker_stop = NULL,
                     offload_path = NULL, configure = list(),
                     follow = NULL) {
  skip_if_no_redis()
  skip_if_not_installed("withr")
  Sys.setenv(R_TESTS = "")

  dir.create(root)
  if (length(sources) > 0) {
    stopifnot(all(file.copy(sources, root)))
    sources <- file.path(root, sources)
  }

  name <- test_name(name)

  create_env <- new.env(parent = globalenv())
  create_env$sources <- sources
  create <- function(envir) {
    for (s in sources) {
      sys.source(s, envir)
    }
  }
  environment(create) <- create_env

  rlang::inject(rrq_configure(name, !!!configure))

  obj <- rrq_controller(name, follow = follow, offload_path = offload_path)

  cfg <- rrq_worker_config(poll_queue = 1, verbose = verbose)
  rrq_worker_config_save(WORKER_CONFIG_DEFAULT, cfg, controller = obj)
  rrq_worker_envir_set(create, controller = obj)

  withr::defer_parent(test_rrq_cleanup(obj, timeout_worker_stop))

  obj
}


test_rrq2 <- function(...) {
  test_rrq(...)
}


test_rrq_cleanup <- function(obj, timeout_worker_stop) {
  if (is.null(timeout_worker_stop)) {
    worker_pid <- vnapply(rrq_worker_info(controller = obj), "[[", "pid")
    worker_is_separate <- worker_pid != Sys.getpid()
    timeout_worker_stop <- if (all(worker_is_separate)) 10 else 0
  }
  rrq_destroy(timeout_worker_stop = timeout_worker_stop, controller = obj)
}


test_worker_spawn <- function(obj, ..., timeout = 10) {
  skip_on_cran()
  skip_on_windows()
  skip_if_installed_version_differs()
  suppressMessages(rrq_worker_spawn(..., timeout = timeout, controller = obj))
}


test_worker_blocking <- function(obj, ...) {
  rrq_worker$new(obj$queue_id, ...)
}


test_worker_watch <- function(queue_id, ...) {
  w <- rrq_worker$new(queue_id, ...)
  w_private <- r6_private(w)
  w_private$verbose <- TRUE
  w$loop()
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

rrq_installed_version <- function() {
  if (!exists("rrq_installed_version", envir = cache, inherits = FALSE)) {
    cache$rrq_installed_version <-
      tryCatch(callr::r(function() utils::packageVersion("rrq")),
               error = function(e) NULL)
  }
  cache$rrq_installed_version
}

skip_if_installed_version_differs <- function() {
  installed <- rrq_installed_version()
  if (is.null(installed)) {
    testthat::skip("rrq not installed locally")
  }
  testing <- utils::packageVersion("rrq")
  if (utils::packageVersion("rrq") != installed) {
    testthat::skip(sprintf(
      "Installed version of rrq (%s) differs to that under test (%s)",
      installed, testing))
  }
}

options(
  ## Need to keep progress off or we get a mess in the tests
  rrq.progress = FALSE,
  ## Cap the task wait timeout so that don't lock up CI with
  ## hard-to-track-down bugs
  rrq.timeout_task_wait = 20)
