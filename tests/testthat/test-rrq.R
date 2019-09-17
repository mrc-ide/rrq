context("rrq")

test_that("empty", {
  obj <- test_rrq()

  expect_is(obj, "rrq_controller")

  expect_equal(obj$worker_list(), character(0))
  expect_equal(obj$task_list(), character(0))
  expect_equal(obj$queue_length(), 0)
  expect_equal(obj$queue_list(), character(0))

  id <- obj$enqueue(sin(1))
  expect_equal(obj$task_list(), id)
  expect_equal(obj$queue_list(), id)
  expect_equal(obj$task_status(id), setNames(TASK_PENDING, id))

  expect_true(
    file.exists(file.path(obj$context$root$path, "bin", "rrq_worker")))

  test_queue_clean(obj$context$id)
})

test_that("basic use", {
  obj <- test_rrq("myfuns.R")

  ## For testing, use: worker_command(obj)
  wid <- test_worker_spawn(obj)

  t <- obj$enqueue(slowdouble(0.1))
  expect_is(t, "character")
  expect_equal(obj$task_wait(t, 2, progress = PROGRESS), 0.2)
  expect_equal(obj$task_result(t), 0.2)

  t <- obj$enqueue(normalizePath(getwd()))
  expect_equal(obj$task_wait(t, 2, progress = PROGRESS),
               normalizePath(obj$context$root$path))
})

test_that("worker name", {
  obj <- test_rrq("myfuns.R")
  root <- obj$context$root$path

  name <- ids::random_id()
  wid <- test_worker_spawn(obj, worker_name_base = name)
  expect_equal(wid, paste0(name, "_1"))
})

test_that("worker timeout", {
  obj <- test_rrq("myfuns.R")

  t <- as.integer(runif(1, min = 100, max = 10000))
  res <- obj$worker_config_save("localhost", timeout = t, copy_redis = TRUE)
  expect_equal(res$timeout, t)

  wid <- test_worker_spawn(obj)

  id <- obj$message_send("TIMEOUT_GET")
  res <- obj$message_get_response(id, wid, timeout = 10)[[1]]
  expect_equal(res[["timeout"]], t)
  expect_lte(res[["remaining"]], t)
  obj$message_send("STOP")

  obj$worker_config_save("infinite", timeout = Inf, copy_redis = TRUE)

  wid <- test_worker_spawn(obj, worker_config = "infinite")
  id <- obj$message_send("TIMEOUT_GET")
  res <- obj$message_get_response(id, wid, timeout = 10)[[1]]
  expect_equal(res[["timeout"]], Inf)
  expect_equal(res[["remaining"]], Inf)
  obj$message_send("STOP")
})

test_that("context job", {
  obj <- test_rrq("myfuns.R")
  context <- obj$context

  ## For testing, use: worker_command(obj)
  wid <- test_worker_spawn(obj)

  id <- context::task_save(quote(sin(1)), context)
  t <- queuer:::queuer_task(id, context$root)

  r <- rrq_controller(context$id, redux::hiredis())

  r$queue_submit(t$id)
  expect_equal(t$wait(10, progress = PROGRESS), sin(1))
  expect_equal(t$status(), "COMPLETE")
  expect_equal(r$queue_length(), 0L)
})

test_that("log dir", {
  obj <- test_rrq("myfuns.R")
  root <- obj$context$root$path

  r <- rrq_controller(obj$context$id, redux::hiredis())

  obj$worker_config_save("localhost", log_path = "worker_logs_task",
                         copy_redis = TRUE)
  worker_command(obj)
  wid <- test_worker_spawn(obj)

  info <- obj$worker_info(wid)[[wid]]
  expect_equal(info$log_path, "worker_logs_task")

  expect_true(file.exists(file.path(root, "worker_logs_task")))

  id <- context::task_save(quote(noisydouble(1)), obj$context)
  t <- queuer:::queuer_task(id, obj$context$root)
  r$queue_submit(t$id)
  res <- t$wait(10, time_poll = 0.1, progress = PROGRESS)

  expect_true(file.exists(file.path(root, obj$db$get(t$id, "log_path"))))
  expect_is(t$log(), "context_log")
  x <- t$log()
  expect_true("start" %in% x$title)
  expect_equal(x$body[[which(x$title == "start")]], "doubling 1")
})

test_that("failed spawn", {
  obj <- test_rrq("myfuns.R")
  root <- obj$context$root$path
  unlink(file.path(root, "myfuns.R"))

  dat <- evaluate_promise(
    try(test_worker_spawn(obj, 2, timeout = 2),
        silent = TRUE))

  expect_is(dat$result, "try-error")
  expect_match(dat$messages, "2 / 2 workers not identified in time",
               all = FALSE, fixed = TRUE)
  expect_match(dat$messages, "Log files recovered for 2 workers",
               all = FALSE, fixed = TRUE)
  ## This might be failing occasionally under covr, but I can't
  ## reproduce
  expect_match(dat$output, "No such file or directory",
               all = FALSE, fixed = TRUE)
})

test_that("error", {
  obj <- test_rrq("myfuns.R")

  wid <- test_worker_spawn(obj)

  t1 <- obj$enqueue(only_positive(1))
  expect_equal(obj$task_wait(t1, 2, progress = PROGRESS), 1)

  t2 <- obj$enqueue(only_positive(-1))
  res <- obj$task_wait(t2, 2, progress = PROGRESS)
  expect_is(res, "rrq_task_error")
  expect_null(res$warnings)

  t3 <- obj$enqueue(nonexistant_function(-1))
  res <- obj$task_wait(t3, 2, progress = PROGRESS)
  expect_is(res, "rrq_task_error")
  expect_null(res$warnings)
})

test_that("error", {
  obj <- test_rrq("myfuns.R")
  context <- obj$context

  wid <- test_worker_spawn(obj)

  t1 <- obj$enqueue(warning_then_error(2))
  r1 <- obj$task_wait(t1, 2, progress = PROGRESS)
  expect_is(r1, "rrq_task_error")
  expect_is(r1, "try-error")
  expect_is(r1$warnings, "list")
  expect_equal(length(r1$warnings), 2)
  expect_is(r1$warnings[[1]], "simpleWarning")
  expect_equal(r1$warnings[[1]]$message, "This is warning number 1")
  expect_equal(r1$warnings[[2]]$message, "This is warning number 2")

  expect_match(tail(r1$trace, 2)[[1]], "^warning_then_error")

  id <- context::task_save(quote(warning_then_error(2)), context)
  obj$queue_submit(id)
  t <- queuer:::queuer_task(id, context$root)
  r2 <- t$wait(10, time_poll = 0.1, progress = PROGRESS)

  expect_is(r2, "context_task_error")
  expect_is(r2$warnings, "list")
  expect_equal(length(r2$warnings), 2)
  expect_is(r2$warnings[[1]], "simpleWarning")
  expect_equal(r2$warnings[[1]]$message, "This is warning number 1")
  expect_equal(r2$warnings[[2]]$message, "This is warning number 2")

  expect_match(tail(r2$trace, 2)[[1]], "^warning_then_error")
})


test_that("task_position", {
  obj <- test_rrq("myfuns.R")

  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(1))
  t3 <- obj$enqueue(sin(1))

  expect_equal(obj$task_position(t1), 1L)
  expect_equal(obj$task_position(c(t1, t2, t3)), c(1L, 2L, 3L))
  expect_equal(obj$task_position("not a real task"), 0L)
  expect_equal(obj$task_position("not a real task", NA_integer_), NA_integer_)
  expect_equal(obj$task_position(c(t1, "not a real task"), NA_integer_),
               c(1L, NA_integer_))
})


test_that("call", {
  obj <- test_rrq("myfuns.R")

  envir <- obj$context$envir
  a <- 20L

  t1 <- obj$call(quote(noisydouble), 10, envir = envir)
  t2 <- obj$call(quote(noisydouble), a, envir = envir)
  t3 <- obj$call(quote(add), a, a, envir = envir)

  wid <- test_worker_spawn(obj)

  expect_equal(obj$task_wait(t1, progress = PROGRESS), 20L)
  expect_equal(obj$task_wait(t2, progress = PROGRESS), 40L)
  expect_equal(obj$task_wait(t3, progress = PROGRESS), 40L)
})
