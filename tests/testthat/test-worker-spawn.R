test_that("Don't wait", {
  obj <- test_rrq()
  res <- test_worker_spawn(obj, 4, timeout = 0)

  expect_s3_class(res, "rrq_worker_manager")
  expect_type(res$id, "character")
  expect_match(res$id, "_[0-9]+$")

  ans <- withVisible(res$wait_alive(timeout = 10, time_poll = 0.1))
  expect_false(ans$visible)
  expect_s3_class(ans$value, "difftime")

  ## Can call again with no ill effect:
  expect_s3_class(
    res$wait_alive(timeout = 10, time_poll = 0.1),
    "difftime")
})


test_that("failed spawn", {
  skip_on_covr()
  skip_on_windows()

  root <- tempfile()
  obj <- test_rrq("myfuns.R", root, verbose = TRUE)
  unlink(file.path(root, "myfuns.R"))

  err <- expect_error(
    suppressMessages(rrq_worker_spawn(timeout = 5, controller = obj)),
    "Worker died")

  expect_length(err$logs, 1)
})


test_that("read worker process log", {
  obj <- test_rrq(verbose = TRUE)
  w <- test_worker_spawn(obj, 1)
  rrq_message_send_and_wait("STOP", controller = obj)
  txt <- rrq_worker_process_log(w$id, controller = obj)
  expect_type(txt, "character")
  expect_match(txt, "ALIVE", all = FALSE)
  expect_equal(txt, w$logs(1))
})


test_that("wait for worker exit", {
  obj <- test_rrq("myfuns.R", timeout_worker_stop = 0)
  w <- test_worker_spawn(obj)

  con <- obj$con # save a copy
  queue_id <- obj$queue_id
  rrq_destroy(timeout_worker_stop = 0.5, controller = obj)

  expect_equal(
    redux::scan_find(con, paste0(queue_id, ":*")),
    character(0))
})


test_that("error if we try to interact with non-managed worker", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_spawn(obj)
  expect_error(
    w$logs(2),
    "Worker not controlled by this manager: '.+_2'")
  expect_error(
    w$logs("fred"),
    "Worker not controlled by this manager: 'fred'")
})


test_that("can wait on manually spawned workers", {
  obj <- test_rrq("myfuns.R")

  queue_id <- obj$queue_id
  worker_ids <- sprintf("%s_%d", ids::adjective_animal(), 1:2)

  expect_error(
    suppressMessages(rrq_worker_wait(worker_ids, timeout = 0, time_poll = 1,
                                     progress = FALSE, controller = obj)),
    "All 2 workers not ready in time")

  p1 <- callr::r_bg(function(queue_id, worker_id) {
    rrq::rrq_worker$new(queue_id, worker_id = worker_id)$loop()
  }, list(queue_id, worker_ids[[1]]), package = TRUE, cleanup = FALSE)
  p2 <- callr::r_bg(function(queue_id, worker_id) {
    rrq::rrq_worker$new(queue_id, worker_id = worker_id)$loop()
  }, list(queue_id, worker_ids[[2]]), package = TRUE, cleanup = FALSE)

  res <- rrq_worker_wait(worker_ids, timeout = 5, time_poll = 0.1,
                         progress = FALSE, controller = obj)
  expect_s3_class(res, "difftime")

  ## If we're unlucky GC will happen within a loop here, so be kind
  ## and try a couple of times. Potentially problemetic as part of the
  ## coverage build, which is very slow.
  testthat::try_again(
    5,
    expect_lt(rrq_worker_wait(worker_ids, timeout = 5, time_poll = 0.1,
                              progress = FALSE, controller = obj),
              0.5))
})


test_that("Can build fallback where no logs present", {
  expect_equal(
    worker_format_failed_logs(list(logs = NULL)),
    c("!" = "Logging not enabled for these workers"))
})


test_that("Can format logs for missing workers", {
  expect_equal(
    worker_format_failed_logs(list(logs = list(alice = c("a", "b")))),
    c(i = "Log files recovered for 1 worker",
      ">" = "alice", "a", "b"))
  expect_equal(
    worker_format_failed_logs(list(logs = list(alice = c("a", "b"),
                                               bob = "c"))),
    c(i = "Log files recovered for 2 workers",
      ">" = "alice", "a", "b", "",
      ">" = "bob", "c"))
})


test_that("can provide informative error messages on worker spawn failure", {
  expect_error(
    abort_workers_not_ready("died", NULL),
    "Worker died")
  expect_error(
    abort_workers_not_ready(rep("died", 3), NULL),
    "All 3 workers died")
  expect_error(
    abort_workers_not_ready(c("died", "died", "ready", "waiting"), NULL),
    "2 / 4 workers died")
  expect_error(
    abort_workers_not_ready("waiting", NULL),
    "Worker not ready in time")
  expect_error(
    abort_workers_not_ready(rep("waiting", 3), NULL),
    "All 3 workers not ready in time")
  expect_error(
    abort_workers_not_ready(c("timeout", "timeout", "running"), NULL),
    "2 / 3 workers not ready in time")
})


test_that("report back correctly", {
  skip_if_not_installed("mockery")

  root <- tempfile()
  obj <- test_rrq("myfuns.R", root, verbose = TRUE)

  worker_ids <- paste(ids::adjective_animal(), 1:2, sep = "_")
  path_logs <- withr::local_tempdir()
  writeLines(c("a", "b"), file.path(path_logs, worker_ids[[1]]))
  writeLines(c("c", "d"), file.path(path_logs, worker_ids[[2]]))

  fetch_logs <- function(id) {
    readLines(file.path(path_logs, id))
  }

  mock_logwatch <- mockery::mock(list(status = c("died", "died")))
  mockery::stub(worker_wait_alive, "logwatch::logwatch", mock_logwatch)
  err <- expect_error(
    worker_wait_alive(obj, worker_ids, is_dead = NULL, fetch_logs = fetch_logs,
                      timeout = 0, time_poll = 1, progress = FALSE),
    "All 2 workers died")

  mockery::expect_called(mock_logwatch, 1)
  expect_equal(err$logs, set_names(list(c("a", "b"), c("c", "d")), worker_ids))
  expect_equal(err$footer, worker_format_failed_logs)
})


test_that("can configure offload path on workers", {
  path <- withr::local_tempdir()
  obj <- test_rrq(offload_threshold_size = 100, offload_path = path)
  res <- test_worker_spawn(obj, 1, offload_path = path)

  workers <- rrq_worker_info(controller = obj)
  expect_equal(workers[[1]]$offload_path, path)

  expect_length(dir(path), 0)

  t <- rrq_task_create_expr(rep(1, 100), controller = obj)
  rrq_task_wait(t, controller = obj)

  # Check that a file has been produced in our offload directory.
  expect_length(dir(path), 1)
})
