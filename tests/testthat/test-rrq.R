test_that("empty", {
  obj <- test_rrq()

  expect_s3_class(obj, "rrq_controller")

  expect_equal(rrq_worker_list(controller = obj), character(0))
  expect_equal(rrq_task_list(controller = obj), character(0))
  expect_equal(rrq_worker_len(controller = obj), 0)
  expect_equal(obj$queue_length(), 0)
  expect_equal(obj$queue_list(), character(0))

  id <- rrq_task_create_expr(sin(1), controller = obj)
  expect_equal(rrq_task_list(controller = obj), id)
  expect_equal(obj$queue_list(), id)
  expect_equal(rrq_task_status(id, controller = obj), TASK_PENDING)

  expect_equal(
    rrq_worker_log_tail(controller = obj),
    data_frame(worker_id = character(0),
               child = integer(0),
               time = numeric(0),
               command = character(0),
               message = character(0)))

  test_queue_clean(obj$queue_id)
})


test_that("basic use", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- rrq_task_create_expr(slowdouble(0.1), controller = obj)
  expect_type(t, "character")
  w$step(TRUE)
  expect_true(rrq_task_wait(t, 2, controller = obj))
  expect_equal(rrq_task_result(t, controller = obj), 0.2)
})


test_that("task errors are returned", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- rrq_task_create_expr(only_positive(1), controller = obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(t1, controller = obj), 1)

  t2 <- rrq_task_create_expr(only_positive(-1), controller = obj)
  w$step(TRUE)
  res <- rrq_task_result(t2, controller = obj)
  expect_s3_class(res, "rrq_task_error")
  expect_null(res$warnings)
  expect_equal(res$task_id, t2)
  expect_equal(res$queue_id, obj$queue_id)
  expect_equal(res$status, TASK_ERROR)

  t3 <- rrq_task_create_expr(nonexistant_function(-1), controller = obj)
  w$step(TRUE)
  res <- rrq_task_result(t3, controller = obj)
  expect_s3_class(res, "rrq_task_error")
  expect_null(res$warnings)
  expect_equal(res$task_id, t3)
  expect_equal(res$queue_id, obj$queue_id)
  expect_equal(res$status, TASK_ERROR)
})


test_that("task warnings are returned", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- rrq_task_create_expr(warning_then_error(2), controller = obj)
  w$step(TRUE)

  r1 <- rrq_task_result(t1, controller = obj)
  expect_s3_class(r1, "rrq_task_error")
  expect_type(r1$warnings, "list")
  expect_equal(length(r1$warnings), 2)
  expect_equal(conditionMessage(r1$warnings[[1]]),
               "This is warning number 1")
  expect_equal(conditionMessage(r1$warnings[[2]]),
               "This is warning number 2")

  expect_s3_class(r1$trace, "rlang_trace")
  expect_match(format(r1$trace), "warning_then_error", all = FALSE)
})


test_that("task_position", {
  obj <- test_rrq("myfuns.R")

  t1 <- rrq_task_create_expr(sin(1), controller = obj)
  t2 <- rrq_task_create_expr(sin(1), controller = obj)
  t3 <- rrq_task_create_expr(sin(1), controller = obj)

  expect_equal(rrq_task_position(t1, controller = obj), 1L)
  expect_equal(rrq_task_position(c(t1, t2, t3), controller = obj),
               c(1L, 2L, 3L))
  expect_equal(rrq_task_position("not a real task", controller = obj), 0L)
  expect_equal(
    rrq_task_position("not a real task", NA_integer_, controller = obj),
    NA_integer_)
  expect_equal(
    rrq_task_position(c(t1, "not a real task"), NA_integer_, controller = obj),
    c(1L, NA_integer_))
})

test_that("task_preceeding", {
  obj <- test_rrq("myfuns.R")

  t1 <- rrq_task_create_expr(sin(1), controller = obj)
  t2 <- rrq_task_create_expr(sin(1), controller = obj)
  t3 <- rrq_task_create_expr(sin(1), controller = obj)

  expect_equal(rrq_task_preceeding(t1, controller = obj), character(0))
  expect_equal(rrq_task_preceeding(t2, controller = obj), t1)
  expect_equal(rrq_task_preceeding(t3, controller = obj), c(t1, t2))
  expect_null(rrq_task_preceeding("not a real task", controller = obj))

  w <- test_worker_spawn(obj)
  rrq_task_wait(t3, controller = obj)
  expect_null(rrq_task_preceeding(t3, controller = obj))
})


test_that("task_position and task_preceeding are consistent", {
  obj <- test_rrq("myfuns.R")

  t1 <- rrq_task_create_expr(sin(1), controller = obj)
  t2 <- rrq_task_create_expr(sin(1), controller = obj)
  t3 <- rrq_task_create_expr(sin(1), controller = obj)

  expect_equal(length(rrq_task_preceeding(t1, controller = obj)), rrq_task_position(t1, controller = obj) - 1)
  expect_equal(length(rrq_task_preceeding(t2, controller = obj)), rrq_task_position(t2, controller = obj) - 1)
  expect_equal(length(rrq_task_preceeding(t3, controller = obj)), rrq_task_position(t3, controller = obj) - 1)
})


test_that("task_overview", {
  obj <- test_rrq("myfuns.R")

  expect_equal(
    rrq_task_overview(controller = obj),
    list(PENDING = 0, RUNNING = 0, COMPLETE = 0, ERROR = 0, CANCELLED = 0,
         DIED = 0, TIMEOUT = 0, IMPOSSIBLE = 0, DEFERRED = 0, MOVED = 0))

  t1 <- rrq_task_create_expr(sin(1), controller = obj)
  t2 <- rrq_task_create_expr(sin(1), controller = obj)
  t3 <- rrq_task_create_expr(sin(1), controller = obj)

  expect_equal(
    rrq_task_overview(controller = obj),
    list(PENDING = 3, RUNNING = 0, COMPLETE = 0, ERROR = 0, CANCELLED = 0,
         DIED = 0, TIMEOUT = 0, IMPOSSIBLE = 0, DEFERRED = 0, MOVED = 0))
})


test_that("wait for tasks", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_spawn(obj)

  t1 <- rrq_task_create_expr(1 + 1, controller = obj)
  t2 <- rrq_task_create_expr(2 + 2, controller = obj)

  expect_true(rrq_task_wait(c(t1, t2), controller = obj))
  expect_equal(rrq_task_results(c(t1, t2), controller = obj), list(2, 4))

  ## Slightly slower jobs:
  t3 <- rrq_task_create_expr(slowdouble(0.1), controller = obj)
  t4 <- rrq_task_create_expr(slowdouble(0.1), controller = obj)
  expect_true(rrq_task_wait(c(t3, t4), controller = obj))
  expect_equal(rrq_task_results(c(t3, t4), controller = obj), list(0.2, 0.2))

  expect_error(
    rrq_task_wait(c(t1, t2), time_poll = -1, controller = obj),
    "'time_poll' cannot be less than 0")
})


test_that("task delete", {
  obj <- test_rrq("myfuns.R")
  t1 <- rrq_task_create_expr(1 + 1, controller = obj)
  t2 <- rrq_task_create_expr(2 + 2, controller = obj)
  t3 <- rrq_task_create_expr(3 + 3, controller = obj)

  expect_setequal(rrq_task_list(controller = obj), c(t1, t2, t3))
  rrq_task_delete(t1, controller = obj)
  expect_setequal(rrq_task_list(controller = obj), c(t2, t3))
  rrq_task_delete(c(t2, t3), controller = obj)
  expect_setequal(rrq_task_list(controller = obj), character(0))
})


test_that("stop worker", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  rrq_worker_stop(controller = obj)
  expect_error(
    w$step(), "BYE", class = "rrq_worker_stop")
})


test_that("Can't read logs unless enabled", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  expect_error(
    rrq_worker_process_log(w$id, controller = obj),
    "Process log not enabled for this worker")
})


test_that("Can't delete running tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  id <- rrq_task_create_expr(sin(1), controller = obj)
  w$poll(TRUE)
  worker_run_task_start(w, r6_private(w), id)
  expect_error(
    rrq_task_delete(id, controller = obj),
    "Can't delete running tasks")
})


test_that("Error if results are not ready", {
  obj <- test_rrq()
  id1 <- rrq_task_create_expr(sin(1), controller = obj)
  id2 <- rrq_task_create_expr(sin(1), controller = obj)
  expect_error(rrq_task_result(id1, controller = obj),
               sprintf("Missing result for task: '%s'", id1))
  expect_error(rrq_task_results(id1, controller = obj),
               sprintf("Missing result for task:\n  - %s", id1))
  expect_error(rrq_task_results(c(id1, id2), controller = obj),
               sprintf("Missing result for task:\n  - %s\n  - %s", id1, id2))
})


test_that("worker load", {
  obj <- test_rrq()
  w1 <- test_worker_blocking(obj)
  w2 <- test_worker_blocking(obj)
  load <- rrq_worker_load(controller = obj)
  expect_s3_class(load, "worker_load")
  expect_setequal(load$worker_id, c(w1$id, w2$id))
  avg <- mean(load)
  expect_true(all(avg["used", ] == 0))
  expect_true(all(avg["available", ] == 1))
})


test_that("change environment", {
  create <- function(value) {
    force(value)
    function(envir) {
      envir$x <- value
    }
  }
  environment(create) <- .GlobalEnv # avoid serialisation warning

  obj <- test_rrq()
  obj$envir(create(1))
  w <- test_worker_blocking(obj)
  expect_equal(r6_private(w)$envir$x, 1)

  obj$envir(NULL)
  w$step(TRUE)
  expect_equal(ls(r6_private(w)$envir), character(0))
})


test_that("queue remove", {
  obj <- test_rrq()
  t1 <- rrq_task_create_expr(sin(1), controller = obj)
  t2 <- rrq_task_create_expr(sin(1), controller = obj)
  t3 <- rrq_task_create_expr(sin(1), controller = obj)

  expect_equal(obj$queue_length(), 3)
  expect_equal(obj$queue_remove(t2), TRUE)
  expect_equal(obj$queue_length(), 2)
  expect_equal(obj$queue_list(), c(t1, t3))

  expect_equal(obj$queue_remove(c(t1, t2, t3)), c(TRUE, FALSE, TRUE))
  expect_equal(obj$queue_remove(character(0)), logical(0))
})


test_that("cancel queued task", {
  obj <- test_rrq()
  t <- rrq_task_create_expr(sqrt(1), controller = obj)
  expect_equal(rrq_task_status(t, controller = obj), TASK_PENDING)
  rrq_task_cancel(t, controller = obj)

  expect_equal(rrq_task_status(t, controller = obj), TASK_CANCELLED)
  expect_equal(obj$queue_list(), character(0))
})


test_that("cancel queued task from alternative queue", {
  obj <- test_rrq()
  t1 <- rrq_task_create_expr(sqrt(1), queue = "other", controller = obj)
  t2 <- rrq_task_create_expr(sqrt(1), queue = "other", controller = obj)
  t3 <- rrq_task_create_expr(sqrt(1), queue = "other", controller = obj)
  expect_equal(obj$queue_list("other"), c(t1, t2, t3))
  rrq_task_cancel(t2, controller = obj)
  expect_equal(obj$queue_list("other"), c(t1, t3))
  expect_equal(rrq_task_status(t2, controller = obj), TASK_CANCELLED)
})


test_that("can't cancel completed task", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t <- rrq_task_create_expr(sqrt(1), controller = obj)
  w$step(TRUE)
  expect_error(
    rrq_task_cancel(t, controller = obj),
    "Task [[:xdigit:]]{32} is not cancelable \\(COMPLETE\\)")
  expect_equal(rrq_task_result(t, controller = obj), 1)
})


test_that("can't cancel nonexistant task", {
  obj <- test_rrq()
  id <- ids::random_id()
  expect_error(
    rrq_task_cancel(id, controller = obj),
    "Task [[:xdigit:]]{32} is not cancelable \\(MISSING\\)")
})


test_that("can't cancel running in-process task", {
  obj <- test_rrq(timeout_worker_stop = 0)
  w <- test_worker_spawn(obj)
  t <- rrq_task_create_expr(Sys.sleep(20), controller = obj)
  wait_status(t, obj)
  expect_error(
    rrq_task_cancel(t, wait = TRUE, controller = obj),
    "Can't cancel running task '[[:xdigit:]]{32}' as not in separate process")
  w$stop(type = "kill_local")
})


test_that("get task info", {
  obj <- test_rrq()

  a <- 1
  b <- 2
  t <- rrq_task_create_expr(log(a, b), controller = obj)
  res <- rrq_task_data(t, controller = obj)
  expect_equal(res$expr, quote(log(a, b)))
  expect_mapequal(res$variables, list(a = 1, b = 2))
})


test_that("get task data errors appropriately if task is missing", {
  obj <- test_rrq()
  id <- ids::random_id()
  expect_error(
    rrq_task_data(id, controller = obj),
    "Task '[[:xdigit:]]+' not found")
})


test_that("a worker will pick up tasks from the priority queue", {
  obj <- test_rrq("myfuns.R")
  cfg <- rrq_worker_config(queue = c("a", "b"), verbose = FALSE)
  rrq_worker_config_save2(WORKER_CONFIG_DEFAULT, cfg, controller = obj)
  w <- test_worker_blocking(obj)

  t1 <- rrq_task_create_expr(sin(1), controller = obj)
  t2 <- rrq_task_create_expr(sin(2), queue = "b", controller = obj)
  t3 <- rrq_task_create_expr(sin(3), queue = "a", controller = obj)

  expect_equal(rrq_task_status(c(t1, t2, t3), controller = obj),
               rep("PENDING", 3))
  expect_equal(obj$queue_list(), t1)
  expect_equal(obj$queue_list("b"), t2)
  expect_equal(obj$queue_list("a"), t3)

  w$step(TRUE)
  expect_equal(rrq_task_status(c(t1, t2, t3), controller = obj),
               c("PENDING", "PENDING", "COMPLETE"))
  w$step(TRUE)
  expect_equal(rrq_task_status(c(t1, t2, t3), controller = obj),
               c("PENDING", "COMPLETE", "COMPLETE"))
  w$step(TRUE)
  expect_equal(rrq_task_status(c(t1, t2, t3), controller = obj),
               c("COMPLETE", "COMPLETE", "COMPLETE"))
})


test_that("Query jobs in different queues", {
  obj <- test_rrq("myfuns.R")

  t1 <- rrq_task_create_expr(sin(1), queue = "a", controller = obj)
  t2 <- rrq_task_create_expr(sin(2), queue = "a", controller = obj)
  t3 <- rrq_task_create_expr(sin(3), queue = "a", controller = obj)

  expect_equal(rrq_task_status(c(t1, t2, t3), controller = obj),
               rep("PENDING", 3))
  expect_equal(obj$queue_list(), character(0))
  expect_equal(obj$queue_list("a"), c(t1, t2, t3))

  expect_equal(rrq_task_position(t1, queue = "a", controller = obj), 1)
  expect_equal(rrq_task_position(t2, queue = "a", controller = obj), 2)

  expect_true(obj$queue_remove(t1, queue = "a"))
  expect_false(obj$queue_remove(t1, queue = "a"))

  expect_equal(rrq_task_position(t2, queue = "a", controller = obj), 1)
  expect_equal(obj$queue_length("a"), 2)
  expect_equal(obj$queue_list("a"), c(t2, t3))
})


test_that("Send job to new process", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  rrq_worker_log_tail(w$id, Inf, controller = obj)

  t <- rrq_task_create_expr(slowdouble(0.1), separate_process = TRUE, controller = obj)
  expect_type(t, "character")
  w$step(TRUE)
  rrq_task_wait(t, timeout = 2, controller = obj)
  expect_equal(rrq_task_result(t, controller = obj), 0.2)
})


## Doing this version does require that we spawn a worker because we
## need to interrupt the secondary worker loop. However, we could do
## an in-process version by being a bit sneaky and writing a job that
## will cancel itself.
test_that("Cancel job sent to new process", {
  obj <- test_rrq("myfuns.R")

  w <- test_worker_spawn(obj)
  t <- rrq_task_create_expr(slowdouble(50), separate_process = TRUE, controller = obj)
  wait_status(t, obj)
  rrq_task_cancel(t, wait = TRUE, controller = obj)
  expect_equal(rrq_task_status(t, controller = obj), TASK_CANCELLED)
  expect_equal(rrq_task_result(t, controller = obj),
               worker_task_failed(TASK_CANCELLED, obj$queue_id, t))

  ## Flakey on covr, probably due to the job being cancelled before
  ## the second process really finishes starting up.
  skip_on_covr()
  log <- rrq_worker_log_tail(w$id, Inf, controller = obj)
  expect_equal(log$command,
               c("ALIVE", "ENVIR", "ENVIR", "QUEUE",
                 "TASK_START", "REMOTE",
                 "CHILD", "ENVIR", "ENVIR", "CANCEL", "TASK_CANCELLED"))
})


test_that("can check task exists", {
  obj <- test_rrq("myfuns.R")
  t <- rrq_task_create_expr(sin(0), controller = obj)
  t2 <- rrq_task_create_expr(sin(0), controller = obj)
  expect_true(rrq_task_exists(t, controller = obj))
  expect_false(rrq_task_exists("123", controller = obj))
  expect_equal(rrq_task_exists(c(t, t2), controller = obj),
               c(TRUE, TRUE))
  expect_equal(rrq_task_exists(c(t, "123"), controller = obj),
               c(TRUE, FALSE))
})


test_that("queue task with missing dependency throws an error", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  expect_error(rrq_task_create_expr(sin(0), depends_on = "123", controller = obj),
               "Failed to queue as dependency 123 does not exist.")
  expect_error(rrq_task_create_expr(sin(0), depends_on = c("123", "456"), controller = obj),
               "Failed to queue as dependencies 123, 456 do not exist.")
})


test_that("task can be queued with dependency", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- rrq_task_create_expr(sin(0), controller = obj)
  t2 <- rrq_task_create_expr(sin(0), controller = obj)
  expect_equal(obj$queue_list(), c(t, t2))

  t3 <- rrq_task_create_expr(sin(pi / 2),
                             depends_on = c(t, t2),
                             controller = obj)
  ## t3 has not been added to main queue yet
  expect_equal(obj$queue_list(), c(t, t2))
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "DEFERRED")

  ## Original dependencies are stored
  original_deps_keys <- rrq_key_task_depends_up_original(obj$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(original_deps_keys), c(t, t2))

  ## Pending dependencies are stored
  dependency_keys <- rrq_key_task_depends_up(obj$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(dependency_keys), c(t, t2))

  ## Inverse depends_on relationship is stored
  dependent_keys <- rrq_key_task_depends_down(obj$queue_id, c(t, t2))
  for (key in dependent_keys) {
    expect_equal(obj$con$SMEMBERS(key), list(t3))
  }

  ## Function to retrieve status of t3 and see what it is waiting for

  w$step(TRUE)
  rrq_task_wait(t, 2, controller = obj)
  expect_equal(unname(rrq_task_status(t, controller = obj)), "COMPLETE")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "PENDING")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "DEFERRED")
  ## Still not on queue
  expect_equal(obj$queue_list(), t2)
  ## Status of it has updated
  expect_setequal(obj$con$SMEMBERS(original_deps_keys), c(t, t2))
  expect_equal(obj$con$SMEMBERS(dependency_keys), list(t2))

  w$step(TRUE)
  rrq_task_wait(t2, 2, controller = obj)
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "COMPLETE")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "PENDING")
  ## Now added to queue
  expect_equal(obj$queue_list(), t3)
  ## Status can be retrieved
  expect_setequal(obj$con$SMEMBERS(original_deps_keys), c(t, t2))
  expect_equal(obj$con$SMEMBERS(dependency_keys), list())

  w$step(TRUE)
  rrq_task_wait(t3, 2, controller = obj)
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "COMPLETE")
})


test_that("task added to queue immediately if dependencies satified", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- rrq_task_create_expr(sin(0), controller = obj)
  expect_equal(obj$queue_list(), t)

  w$step(TRUE)
  rrq_task_wait(t, 2, controller = obj)

  ## Immediately added to queue as t has completed
  t2 <- rrq_task_create_expr(sin(pi / 2), depends_on = t, controller = obj)
  expect_equal(obj$queue_list(), t2)
  expect_equal(unname(rrq_task_status(t2, controller = obj)), TASK_PENDING)
})


test_that("queueing with depends_on errored task fails", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- rrq_task_create_expr(only_positive(-1), controller = obj)
  w$step(TRUE)
  res <- rrq_task_result(t1, controller = obj)
  expect_s3_class(res, "rrq_task_error")

  expect_error(rrq_task_create_expr(sin(0), depends_on = t1, controller = obj),
               paste0("Failed to queue as dependent tasks failed:\n",
                      t1, ": ERROR"),
               fixed = TRUE)

  ids <- rrq_task_list(controller = obj)
  expect_length(ids, 2)
  t2 <- setdiff(ids, t1)

  ## Task is set to impossible
  expect_equal(rrq_task_status(t1, controller = obj), TASK_ERROR)
  expect_equal(rrq_task_status(t2, controller = obj), TASK_IMPOSSIBLE)
})


test_that("dependent tasks updated if dependency fails", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- rrq_task_create_expr(only_positive(-1), controller = obj)
  t2 <- rrq_task_create_expr(sin(0), depends_on = t, controller = obj)
  t3 <- rrq_task_create_expr(sin(0), depends_on = t, controller = obj)
  t4 <- rrq_task_create_expr(sin(0), depends_on = t2, controller = obj)

  expect_equal(obj$queue_list(), t)
  expect_equal(unname(rrq_task_status(t, controller = obj)), "PENDING")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "DEFERRED")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "DEFERRED")
  expect_equal(unname(rrq_task_status(t4, controller = obj)), "DEFERRED")

  w$step(TRUE)
  rrq_task_wait(t, 2, controller = obj)
  expect_equal(unname(rrq_task_status(t, controller = obj)), "ERROR")

  ## Dependent task updated and nothing queued
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "IMPOSSIBLE")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "IMPOSSIBLE")
  expect_equal(unname(rrq_task_status(t4, controller = obj)), "IMPOSSIBLE")
  expect_equal(obj$queue_list(), character(0))
})


test_that("multiple tasks can be queued with same dependency", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- rrq_task_create_expr(sin(0), controller = obj)
  t2 <- rrq_task_create_expr(sin(0), depends_on = t, controller = obj)
  t3 <- rrq_task_create_expr(sin(0), depends_on = t, controller = obj)
  expect_equal(obj$queue_list(), t)
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "DEFERRED")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "DEFERRED")

  w$step(TRUE)
  rrq_task_wait(t, 2, controller = obj)
  expect_equal(unname(rrq_task_status(t, controller = obj)), "COMPLETE")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "PENDING")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "PENDING")
  expect_setequal(obj$queue_list(), c(t2, t3))
})


test_that("deferred task is added to specified queue", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- rrq_task_create_expr(sin(0), controller = obj)
  t2 <- rrq_task_create_expr(sin(0), depends_on = t, controller = obj)
  t3 <- rrq_task_create_expr(sin(0), depends_on = t, queue = "a", controller = obj)
  expect_equal(obj$queue_list(), t)
  expect_equal(obj$queue_list("a"), character(0))

  w$step(TRUE)
  rrq_task_wait(t, 2, controller = obj)

  expect_equal(obj$queue_list(), t2)
  expect_equal(obj$queue_list("a"), t3)
})


test_that("task set to impossible cannot be added to queue", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- rrq_task_create_expr(only_positive(-1), controller = obj)
  t2 <- rrq_task_create_expr(sin(0), controller = obj)
  t3 <- rrq_task_create_expr(sin(pi / 2), depends_on = c(t, t2), controller = obj)
  expect_equal(unname(rrq_task_status(t, controller = obj)), "PENDING")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "PENDING")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "DEFERRED")
  expect_equal(obj$queue_list(), c(t, t2))

  w$step(TRUE)
  res <- rrq_task_result(t, controller = obj)
  expect_s3_class(res, "rrq_task_error")

  expect_equal(unname(rrq_task_status(t, controller = obj)), "ERROR")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "PENDING")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "IMPOSSIBLE")
  expect_equal(obj$queue_list(), t2)

  w$step(TRUE)
  rrq_task_wait(t2, 2, controller = obj)

  expect_equal(unname(rrq_task_status(t2, controller = obj)), "COMPLETE")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "IMPOSSIBLE")
  expect_equal(obj$queue_list(), character(0))
})


test_that("deferred task delete", {
  obj <- test_rrq("myfuns.R")
  t1 <- rrq_task_create_expr(1 + 1, controller = obj)
  t2 <- rrq_task_create_expr(2 + 2, depends_on = t1, controller = obj)
  t3 <- rrq_task_create_expr(2 + 2, depends_on = t1, controller = obj)
  t4 <- rrq_task_create_expr(2 + 2, depends_on = t3, controller = obj)

  expect_setequal(rrq_task_list(controller = obj), c(t1, t2, t3, t4))
  expect_equal(obj$queue_list(), t1)

  rrq_task_delete(t2, controller = obj)
  expect_setequal(rrq_task_list(controller = obj), c(t1, t3, t4))
  expect_equal(obj$queue_list(), t1)
  expect_equal(unname(rrq_task_status(t1, controller = obj)), "PENDING")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "MISSING")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "DEFERRED")
  expect_equal(unname(rrq_task_status(t4, controller = obj)), "DEFERRED")

  rrq_task_delete(t1, controller = obj)
  expect_setequal(rrq_task_list(controller = obj), c(t3, t4))
  expect_setequal(obj$queue_list(), character(0))
  expect_equal(unname(rrq_task_status(t1, controller = obj)), "MISSING")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "MISSING")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "IMPOSSIBLE")
  expect_equal(unname(rrq_task_status(t4, controller = obj)), "IMPOSSIBLE")

  rrq_task_delete(t3, controller = obj)
  expect_setequal(rrq_task_list(controller = obj), t4)
  expect_setequal(obj$queue_list(), character(0))
  expect_equal(unname(rrq_task_status(t1, controller = obj)), "MISSING")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "MISSING")
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "MISSING")
  expect_equal(unname(rrq_task_status(t4, controller = obj)), "IMPOSSIBLE")
})


test_that("delete completed task does not clear dependencies", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  t1 <- rrq_task_create_expr(1 + 1, controller = obj)
  t2 <- rrq_task_create_expr(2 + 2, depends_on = t1, controller = obj)

  w$step(TRUE)
  rrq_task_wait(t1, 2, controller = obj)

  expect_setequal(rrq_task_list(controller = obj), c(t1, t2))
  expect_equal(obj$queue_list(), t2)
  expect_equal(unname(rrq_task_status(t1, controller = obj)), "COMPLETE")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "PENDING")

  rrq_task_delete(t1, controller = obj)
  expect_setequal(rrq_task_list(controller = obj), t2)
  expect_equal(obj$queue_list(), t2)
  expect_equal(unname(rrq_task_status(t1, controller = obj)), "MISSING")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "PENDING")
})


test_that("deferred task cancel", {
  obj <- test_rrq("myfuns.R")
  t1 <- rrq_task_create_expr(1 + 1, controller = obj)
  t2 <- rrq_task_create_expr(2 + 2, depends_on = t1, controller = obj)
  t3 <- rrq_task_create_expr(2 + 2, depends_on = t1, controller = obj)
  t4 <- rrq_task_create_expr(2 + 2, depends_on = t3, controller = obj)

  expect_setequal(rrq_task_list(controller = obj), c(t1, t2, t3, t4))
  expect_equal(obj$queue_list(), t1)

  rrq_task_cancel(t2, controller = obj)
  expect_setequal(rrq_task_list(controller = obj), c(t1, t2, t3, t4))
  expect_equal(obj$queue_list(), t1)
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "DEFERRED")
  expect_equal(unname(rrq_task_status(t4, controller = obj)), "DEFERRED")

  rrq_task_cancel(t1, controller = obj)
  expect_setequal(rrq_task_list(controller = obj), c(t1, t2, t3, t4))
  expect_setequal(obj$queue_list(), character(0))
  expect_equal(unname(rrq_task_status(t3, controller = obj)), "IMPOSSIBLE")
  expect_equal(unname(rrq_task_status(t4, controller = obj)), "IMPOSSIBLE")
})

test_that("can get deferred tasks", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- rrq_task_create_expr(1 + 1, controller = obj)
  t2 <- rrq_task_create_expr(1 + 1, controller = obj)
  t3 <- rrq_task_create_expr(2 + 2, depends_on = c(t1, t2), controller = obj)
  t4 <- rrq_task_create_expr(2 + 2, depends_on = t1, controller = obj)

  tasks <- obj$deferred_list()
  expect_setequal(names(tasks), c(t3, t4))
  expect_setequal(names(tasks[[t3]]), c(t1, t2))
  expect_equal(tasks[[t3]][[t1]], "PENDING")
  expect_equal(tasks[[t3]][[t2]], "PENDING")
  expect_equal(names(tasks[[t4]]), t1)
  expect_equal(tasks[[t4]][[t1]], "PENDING")

  w$step(TRUE)
  rrq_task_wait(t1, 2, controller = obj)

  tasks <- obj$deferred_list()
  expect_equal(names(tasks), t3)
  expect_setequal(names(tasks[[t3]]), c(t1, t2))
  expect_equal(tasks[[t3]][[t1]], "COMPLETE")
  expect_equal(tasks[[t3]][[t2]], "PENDING")

  w$step(TRUE)
  w$step(TRUE)
  rrq_task_wait(t2, 2, controller = obj)
  rrq_task_wait(t4, 2, controller = obj)

  tasks <- obj$deferred_list()
  expect_setequal(obj$deferred_list(), list())
})


test_that("can use task_wait with impossible tasks", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- rrq_task_create_expr(only_positive(-1), controller = obj)
  t2 <- rrq_task_create_expr(sin(0), depends_on = t, controller = obj)

  expect_equal(obj$queue_list(), t)
  expect_equal(unname(rrq_task_status(t, controller = obj)), "PENDING")
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "DEFERRED")

  w$step(TRUE)
  rrq_task_wait(t, 2, controller = obj)
  out <- rrq_task_result(t, controller = obj)
  expect_equal(rrq_task_status(t, controller = obj), "ERROR")
  expect_equal(out$task_id, t)
  expect_equal(out$queue_id, obj$queue_id)

  ## task wait returns for an impossible task
  expect_equal(unname(rrq_task_status(t2, controller = obj)), "IMPOSSIBLE")
  rrq_task_wait(t2, 2, controller = obj)
  out <- rrq_task_result(t2, controller = obj)
  ## Not just a timeout error - it has returned
  expect_s3_class(out, "rrq_task_error")
  expect_equal(out$message, "Task not successful: IMPOSSIBLE")
  expect_equal(out$task_id, t2)
  expect_equal(out$queue_id, obj$queue_id)
})


test_that("submit a task with a timeout requires separate process", {
  obj <- test_rrq("myfuns.R")
  expect_error(
    rrq_task_create_expr(slowdouble(10), timeout_task_run = 1, controller = obj),
    "Can't set timeout as 'separate_process' is FALSE")
})


test_that("submit a task with a timeout", {
  obj <- test_rrq("myfuns.R")
  t <- rrq_task_create_expr(slowdouble(10),
                            timeout_task_run = 1,
                            separate_process = TRUE,
                            controller = obj)
  expect_equal(obj$con$HGET(queue_keys(obj)$task_timeout, t), "1")

  w <- test_worker_blocking(obj)
  w$step(TRUE)

  expect_equal(rrq_task_status(t, controller = obj), TASK_TIMEOUT)
  expect_equal(rrq_task_result(t, controller = obj),
               worker_task_failed(TASK_TIMEOUT, obj$queue_id, t))

  ## Flakey on covr, probably due to the job being cancelled before
  ## the second process really finishes starting up.
  skip_on_covr()
  expect_equal(rrq_worker_log_tail(w$id, Inf, controller = obj)$command,
               c("ALIVE", "ENVIR", "ENVIR", "QUEUE",
                 "TASK_START", "REMOTE",
                 "CHILD", "ENVIR", "ENVIR", "TIMEOUT", "TASK_TIMEOUT"))
})


test_that("can offload storage", {
  skip_if_no_redis()
  name <- sprintf("rrq:%s", ids::random_id())

  path <- tempfile()

  obj <- test_rrq(store_max_size = 100, offload_path = path)
  a <- 10
  b <- runif(20)
  t <- rrq_task_create_expr(sum(b) / a, controller = obj)

  w <- test_worker_blocking(obj)
  w$step(TRUE)

  expect_equal(rrq_task_result(t, controller = obj), sum(b) / a)

  ## Did successfully offload data:
  store <- r6_private(obj)$store
  h <- store$list()
  expect_length(h, 3)
  expect_setequal(store$location(h), c("redis", "offload"))
  expect_equal(store$tags(), t)
  expect_length(dir(path), 1L)
  expect_true(dir(path) %in% h)

  rrq_task_delete(t, controller = obj)

  expect_equal(store$list(), character(0))
  expect_equal(dir(path), character(0))
})


test_that("offload storage in result", {
  path <- tempfile()
  obj <- test_rrq(store_max_size = 100, offload_path = path)
  t <- rrq_task_create_expr(rep(1, 100), controller = obj)

  w <- test_worker_blocking(obj)
  w$step(TRUE)

  expect_equal(rrq_task_result(t, controller = obj), rep(1, 100))

  ## Did successfully offload data:
  store <- r6_private(obj)$store
  h <- store$list()
  expect_length(h, 1)
  expect_setequal(store$location(h), "offload")
  expect_equal(store$tags(), t)
  expect_equal(dir(path), h)

  rrq_task_delete(t, controller = obj)

  expect_equal(store$list(), character(0))
  expect_equal(dir(path), character(0))
})


test_that("collect times", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- rrq_task_create_expr(slowdouble(0.1), controller = obj)
  t2 <- rrq_task_create_expr(slowdouble(0.1), controller = obj)
  tt <- c(t1, t2)

  times1 <- rrq_task_times(tt, controller = obj)
  expect_true(is.matrix(times1)) # testthat 3e makes this quite hard
  expect_equal(dimnames(times1),
               list(tt, c("submit", "start", "complete", "moved")))
  expect_type(times1, "double")
  expect_equal(times1[tt, "start"], set_names(rep(NA_real_, 2), tt))
  expect_equal(times1[tt, "complete"], set_names(rep(NA_real_, 2), tt))
  expect_equal(times1[tt, "moved"], set_names(rep(NA_real_, 2), tt))
  expect_false(any(is.na(times1[tt, "submit"])))

  w$step(TRUE)
  times2 <- rrq_task_times(tt, controller = obj)
  expect_equal(times2[t2, , drop = FALSE], times1[t2, , drop = FALSE])
  expect_equal(rrq_task_times(t2, controller = obj), times1[t2, , drop = FALSE])
  expect_equal(is.na(times2[t1, ]),
               c(submit = FALSE, start = FALSE, complete = FALSE, moved = TRUE))
})


test_that("task errors can be immediately thrown", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- rrq_task_create_expr(only_positive(-1), controller = obj)
  w$step(TRUE)
  err <- expect_error(rrq_task_result(t, error = TRUE, controller = obj),
                      class = "rrq_task_error")
  expect_equal(err$queue_id, obj$queue_id)
  expect_equal(err$task_id, t)

  err2 <- expect_error(
    rrq_task_results(c(t, t), error = TRUE, controller = obj),
    class = "rrq_task_error_group")
  expect_equal(err2$errors, list(err, err))

  expect_equal(
    format(err2),
    c("<rrq_task_error_group>",
      paste("  2/2 tasks failed:",
            "    - x must be positive",
            "    - x must be positive",
            sep = "\n"),
      "  * To throw this error, use stop() with it",
      "  * To inspect individual errors, see $errors"))
})


test_that("Can set the task wait timeout on controller creation", {
  obj <- test_rrq()

  f <- function(timeout) {
    r <- rrq_controller$new(obj$queue_id, timeout_task_wait = timeout)
    r6_private(r)$timeout_task_wait
  }

  withr::with_options(list(rrq.timeout_task_wait = 30), {
    expect_equal(f(10), 10)
    expect_equal(f(NULL), 30)
  })

  withr::with_options(list(rrq.timeout_task_wait = NULL), {
    expect_equal(f(10), 10)
    expect_equal(f(NULL), Inf)
  })
})


test_that("Can get information about a task in the same process", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t <- rrq_task_create_expr(sqrt(4), controller = obj)

  d1 <- rrq_task_info(t, controller = obj)
  expect_setequal(
    names(d1),
    c("id", "status", "queue", "separate_process", "timeout", "worker", "pid",
      "moved", "depends"))
  expect_equal(d1$id, t)
  expect_equal(d1$status, TASK_PENDING)
  expect_equal(d1$queue, "default")
  expect_false(d1$separate_process)
  expect_null(d1$timeout)
  expect_null(d1$worker)
  expect_null(d1$pid)
  expect_equal(d1$moved, list(up = NULL, down = NULL))
  expect_equal(d1$depends, list(up = NULL, down = NULL))

  w$step(TRUE)
  d2 <- rrq_task_info(t, controller = obj)
  expect_setequal(names(d2), names(d1))
  expect_equal(d2$id, t)
  expect_equal(d2$status, TASK_COMPLETE)
  expect_equal(d2$queue, "default")
  expect_false(d2$separate_process)
  expect_null(d2$timeout, 5)
  expect_equal(d2$worker, w$id)
  expect_null(d2$pid, "integer")
  expect_equal(d2$moved, list(up = NULL, down = NULL))
  expect_equal(d2$depends, list(up = NULL, down = NULL))
})


test_that("Can get information about a task in a different process", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t <- rrq_task_create_expr(sqrt(4), separate_process = TRUE, timeout_task_run = 5, controller = obj)

  d1 <- rrq_task_info(t, controller = obj)
  expect_setequal(
    names(d1),
    c("id", "status", "queue", "separate_process", "timeout", "worker", "pid",
      "moved", "depends"))
  expect_equal(d1$id, t)
  expect_equal(d1$status, TASK_PENDING)
  expect_equal(d1$queue, "default")
  expect_true(d1$separate_process)
  expect_equal(d1$timeout, 5)
  expect_null(d1$worker)
  expect_null(d1$pid)
  expect_equal(d1$moved, list(up = NULL, down = NULL))
  expect_equal(d1$depends, list(up = NULL, down = NULL))

  w$step(TRUE)
  d2 <- rrq_task_info(t, controller = obj)
  expect_setequal(names(d2), names(d1))
  expect_equal(d2$id, t)
  expect_equal(d2$status, TASK_COMPLETE)
  expect_equal(d2$queue, "default")
  expect_true(d2$separate_process)
  expect_equal(d2$timeout, 5)
  expect_equal(d2$worker, w$id)
  expect_type(d2$pid, "integer")
  expect_equal(d2$moved, list(up = NULL, down = NULL))
  expect_equal(d2$depends, list(up = NULL, down = NULL))
})


test_that("Can get information about task retries", {
  obj <- test_rrq()
  t <- list()
  w <- test_worker_blocking(obj)

  t1 <- rrq_task_create_expr(identity(1), controller = obj)
  w$step(TRUE)
  expect_mapequal(rrq_task_info(t1, controller = obj)$moved, list(up = NULL, down = NULL))

  t2 <- rrq_task_retry(t1, controller = obj)
  w$step(TRUE)
  expect_mapequal(rrq_task_info(t1, controller = obj)$moved, list(up = NULL, down = t2))
  expect_mapequal(rrq_task_info(t2, controller = obj)$moved, list(up = t1, down = NULL))

  t3 <- rrq_task_retry(t2, controller = obj)
  w$step(TRUE)
  expect_mapequal(rrq_task_info(t1, controller = obj)$moved, list(up = NULL, down = c(t2, t3)))
  expect_mapequal(rrq_task_info(t2, controller = obj)$moved, list(up = t1, down = t3))
  expect_mapequal(rrq_task_info(t3, controller = obj)$moved, list(up = c(t1, t2), down = NULL))

  t4 <- rrq_task_retry(t3, controller = obj)
  w$step(TRUE)
  expect_mapequal(rrq_task_info(t1, controller = obj)$moved,
                  list(up = NULL, down = c(t2, t3, t4)))
  expect_mapequal(rrq_task_info(t2, controller = obj)$moved,
                  list(up = t1, down = c(t3, t4)))
  expect_mapequal(rrq_task_info(t3, controller = obj)$moved,
                  list(up = c(t1, t2), down = t4))
  expect_mapequal(rrq_task_info(t4, controller = obj)$moved,
                  list(up = c(t1, t2, t3), down = NULL))
})


test_that("Can retry tasks that span multiple queues at once", {
  obj <- test_rrq()
  cfg <- rrq_worker_config(queue = c("a", "b"), verbose = FALSE)
  rrq_worker_config_save2(WORKER_CONFIG_DEFAULT, cfg, controller = obj)
  t1 <- c(rrq_task_create_expr(sin(1), queue = "a", controller = obj),
          rrq_task_create_expr(sin(2), queue = "a", controller = obj),
          rrq_task_create_expr(sin(3), queue = "b", controller = obj))

  w <- test_worker_blocking(obj)
  for (i in 1:3) {
    w$step(TRUE)
  }

  t2 <- rrq_task_retry(t1, controller = obj)

  expect_equal(rrq_task_status(t2, controller = obj), rep(TASK_PENDING, 3))
  expect_equal(obj$queue_list("a"), t2[1:2])
  expect_equal(obj$queue_list("b"), t2[3])

  expect_equal(rrq_task_position(t2, queue = "a", controller = obj), c(1, 2, 0))
  expect_equal(rrq_task_position(t2, queue = "b", controller = obj), c(0, 0, 1))

  ## Can also get the position from the old ids:
  expect_equal(rrq_task_position(t1, queue = "a", controller = obj), c(1, 2, 0))
  expect_equal(rrq_task_position(t1, queue = "b", controller = obj), c(0, 0, 1))
  expect_equal(rrq_task_position(t1, queue = "a", follow = FALSE, controller = obj), c(0, 0, 0))
  expect_equal(rrq_task_position(t1, queue = "b", follow = FALSE, controller = obj), c(0, 0, 0))
})


test_that("can get task position and preceeding from retried tasks", {
  obj <- test_rrq()
  t1 <- rrq_task_create_expr(sin(1), controller = obj)
  w <- test_worker_blocking(obj)
  w$step()

  ta <- c(rrq_task_create_expr(sin(2), controller = obj),
          rrq_task_create_expr(sin(3), controller = obj))
  t2 <- rrq_task_retry(t1, controller = obj)
  tb <- c(rrq_task_create_expr(sin(4), controller = obj),
          rrq_task_create_expr(sin(5), controller = obj),
          rrq_task_create_expr(sin(6), controller = obj))

  expect_equal(rrq_task_position(t2, controller = obj), 3)
  expect_equal(rrq_task_position(t1, controller = obj), 3)
  expect_equal(rrq_task_position(t1, follow = FALSE, controller = obj), 0)

  expect_equal(rrq_task_preceeding(t2, controller = obj), ta)
  expect_equal(rrq_task_preceeding(t1, controller = obj), ta)
  expect_null(rrq_task_preceeding(t1, follow = FALSE, controller = obj))
})


test_that("Can set the follow default on controller creation", {
  obj <- test_rrq()

  f <- function(follow) {
    r <- rrq_controller$new(obj$queue_id, follow = follow)
    r6_private(r)$follow
  }

  withr::with_options(list(rrq.follow = FALSE), {
    expect_equal(f(TRUE), TRUE)
    expect_equal(f(FALSE), FALSE)
    expect_equal(f(NULL), FALSE)
  })

  withr::with_options(list(rrq.follow = NULL), {
    expect_equal(f(TRUE), TRUE)
    expect_equal(f(FALSE), FALSE)
    expect_equal(f(NULL), TRUE)
  })
})


test_that("Can avoid following on controller creation", {
  obj1 <- test_rrq(follow = FALSE)
  obj2 <- rrq_controller$new(obj1$queue_id, follow = TRUE)
  w <- test_worker_blocking(obj1)

  t1 <- obj1$enqueue(runif(1))
  w$step(TRUE)

  t2 <- obj1$task_retry(t1)
  expect_equal(obj1$task_status(t1), set_names(TASK_MOVED, t1))
  expect_equal(obj2$task_status(t1), set_names(TASK_PENDING, t1))
})


test_that("Running in separate process produces coherent logs", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  log0 <- rrq_worker_log_tail(w$id, Inf, controller = obj)

  t <- rrq_task_create_expr(runif(1), separate_process = TRUE, controller = obj)
  expect_type(t, "character")
  w$step(TRUE)

  expect_equal(rrq_task_status(t, controller = obj), TASK_COMPLETE)

  log <- rrq_worker_log_tail(w$id, Inf, controller = obj)
  expect_equal(log[seq_len(nrow(log0)), ], log0)

  log1 <- log[-seq_len(nrow(log0)), ]

  expect_true(all(log1$worker_id == w$id))
  expect_equal(
    log1$command,
    c("TASK_START", "REMOTE",
      "CHILD", "ENVIR", "ENVIR", "STOP",
      "TASK_COMPLETE"))
  expect_equal(
    is.na(log1$child),
    c(TRUE, TRUE, FALSE, FALSE, FALSE, FALSE, TRUE))
})


test_that("Can extract dependency information", {
  obj <- test_rrq()
  t1 <- rrq_task_create_expr(identity(1), controller = obj)
  t2 <- rrq_task_create_expr(identity(2), depends_on = t1, controller = obj)
  t3 <- rrq_task_create_expr(identity(3), depends_on = t1, controller = obj)
  t4 <- rrq_task_create_expr(identity(4), depends_on = c(t1, t2), controller = obj)
  t5 <- rrq_task_create_expr(identity(4), depends_on = c(t4, t3), controller = obj)

  deps1 <- rrq_task_info(t1, controller = obj)$depends
  deps2 <- rrq_task_info(t2, controller = obj)$depends
  deps3 <- rrq_task_info(t3, controller = obj)$depends
  deps4 <- rrq_task_info(t4, controller = obj)$depends
  deps5 <- rrq_task_info(t5, controller = obj)$depends

  expect_null(deps1$up)
  expect_setequal(names(deps1$down), c(t1, t2, t3, t4))
  expect_setequal(deps1$down[[t1]], c(t2, t3, t4))

  expect_equal(deps2$up, set_names(list(t1), t2))
  expect_equal(deps2$down, set_names(list(t4, t5), c(t2, t4)))

  expect_equal(deps3$up, set_names(list(t1), t3))
  expect_equal(deps3$down, set_names(list(t5), t3))

  expect_setequal(names(deps4$up), c(t4, t2))
  expect_setequal(deps4$up[[t4]], c(t1, t2))
  expect_equal(deps4$up[[t2]], deps2$up[[t2]])
  expect_equal(deps4$down, set_names(list(t5), t4))

  expect_setequal(names(deps5$up), c(t5, t4, t3, t2))
  expect_setequal(deps5$up[[t5]], c(t4, t3))
  expect_equal(deps5$up[[t4]], deps4$up[[t4]])
  expect_equal(deps5$up[[t3]], deps3$up[[t3]])
  expect_equal(deps5$up[[t2]], deps2$up[[t2]])
  expect_null(deps5$depends$down)
})
