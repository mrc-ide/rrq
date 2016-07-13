context("rrq")

test_that("sanity checking", {
  root <- tempfile()
  context <- context::context_save(root)

  obj <- rrq_controller(context, redux::hiredis())
  expect_equal(obj$workers_list(), character(0))
  expect_equal(obj$tasks_list(), character(0))
  expect_equal(obj$queue_length(), 0)
  expect_equal(obj$queue_list(), character(0))

  id <- obj$enqueue(sin(1))
  expect_equal(obj$tasks_list(), id)
  expect_equal(obj$queue_list(), id)
  expect_equal(unname(obj$tasks_status(id)), TASK_PENDING)

  test_queue_clean(context$id)
})

test_that("basic use", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  wid <- workers_spawn(obj$context, obj$con)

  t <- obj$enqueue(slowdouble(0.1))
  expect_is(t, "character")
  expect_equal(obj$task_wait(t, 2), 0.2)
  expect_equal(obj$task_result(t), 0.2)
})

test_that("worker name", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  name <- ids::random_id()
  wid <- workers_spawn(obj$context, obj$con,
                      logdir="logs", worker_name_base=name)
  expect_equal(wid, paste0(name, "_1"))
  expect_true(file.exists(file.path("logs", paste0(name, "_1.log"))))
})

test_that("worker timeout", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  t <- as.integer(runif(1, min=100, max=10000))
  wid <- workers_spawn(obj$context, obj$con,
                       logdir="logs", worker_timeout=t)
  id <- obj$send_message("TIMEOUT_GET")
  res <- obj$get_response(id, wid, wait=10)
  expect_equal(res[["timeout"]], t)
  expect_lte(res[["remaining"]], t)
  obj$send_message("STOP")

  wid <- workers_spawn(obj$context, obj$con,
                       logdir="logs", worker_timeout=Inf)
  id <- obj$send_message("TIMEOUT_GET")
  res <- obj$get_response(id, wid, wait=10)
  expect_equal(res[["timeout"]], Inf)
  expect_equal(res[["remaining"]], Inf)
  obj$send_message("STOP")
})

test_that("log dir", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  r <- worker_controller(context$id, redux::hiredis())

  on.exit(obj$destroy())

  wid <- workers_spawn(obj$context, obj$con,
                       logdir="logs", worker_log_path="logs_worker")


  expect_true(file.exists(file.path(root, "logs_worker")))

  t <- context::task_save(quote(sin(1)), context)
  tt <- queuer:::task(context, t$id)

  r$queue_submit(t$id)
  res <- tt$wait(10)

  logf <- file.path(root, "logs_worker", t$id)
  expect_true(file.exists(logf))
  log <- readLines(logf)
  expect_true(any(grepl("[ expr", log, fixed=TRUE)))
  expect_is(tt$log(), "context_log")
})
