test_that("empty", {
  obj <- test_rrq()

  expect_s3_class(obj, "rrq_controller")

  expect_equal(obj$worker_list(), character(0))
  expect_equal(obj$task_list(), character(0))
  expect_equal(obj$worker_len(), 0)
  expect_equal(obj$queue_length(), 0)
  expect_equal(obj$queue_list(), character(0))

  id <- obj$enqueue(sin(1))
  expect_equal(obj$task_list(), id)
  expect_equal(obj$queue_list(), id)
  expect_equal(obj$task_status(id), setNames(TASK_PENDING, id))

  expect_equal(
    obj$worker_log_tail(),
    data_frame(worker_id = character(0),
               time = numeric(0),
               command = character(0),
               message = character(0)))

  test_queue_clean(obj$queue_id)
})


test_that("basic use", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(slowdouble(0.1))
  expect_type(t, "character")
  w$step(TRUE)
  expect_equal(obj$task_wait(t, 2), 0.2)
  expect_equal(obj$task_result(t), 0.2)
})


test_that("task errors are returned", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(only_positive(1))
  w$step(TRUE)
  expect_equal(obj$task_result(t1), 1)

  t2 <- obj$enqueue(only_positive(-1))
  w$step(TRUE)
  res <- obj$task_result(t2)
  expect_s3_class(res, "rrq_task_error")
  expect_null(res$warnings)
  expect_equal(res$task_id, t2)
  expect_equal(res$queue_id, obj$queue_id)
  expect_equal(res$status, TASK_ERROR)

  t3 <- obj$enqueue(nonexistant_function(-1))
  w$step(TRUE)
  res <- obj$task_result(t3)
  expect_s3_class(res, "rrq_task_error")
  expect_null(res$warnings)
  expect_equal(res$task_id, t3)
  expect_equal(res$queue_id, obj$queue_id)
  expect_equal(res$status, TASK_ERROR)
})


test_that("task warnings are returned", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(warning_then_error(2))
  ## Pretty ugly here with testthat v3:
  expect_warning(expect_warning(w$step(TRUE),
                                "This is warning number 1"),
                 "This is warning number 2")

  r1 <- obj$task_result(t1)
  expect_s3_class(r1, "rrq_task_error")
  expect_type(r1$warnings, "character")
  expect_equal(length(r1$warnings), 2)
  expect_equal(r1$warnings, sprintf("This is warning number %d", 1:2))

  expect_s3_class(r1$trace, "rlang_trace")
  expect_match(format(r1$trace), "warning_then_error", all = FALSE)
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

test_that("task_preceeding", {
  obj <- test_rrq("myfuns.R")

  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(1))
  t3 <- obj$enqueue(sin(1))

  expect_equal(obj$task_preceeding(t1), character(0))
  expect_equal(obj$task_preceeding(t2), t1)
  expect_equal(obj$task_preceeding(t3), c(t1, t2))
  expect_null(obj$task_preceeding("not a real task"))

  wid <- test_worker_spawn(obj)
  obj$task_wait(t3)
  expect_null(obj$task_preceeding(t3))
})


test_that("task_position and task_preceeding are consistent", {
  obj <- test_rrq("myfuns.R")

  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(1))
  t3 <- obj$enqueue(sin(1))

  expect_equal(length(obj$task_preceeding(t1)), obj$task_position(t1) - 1)
  expect_equal(length(obj$task_preceeding(t2)), obj$task_position(t2) - 1)
  expect_equal(length(obj$task_preceeding(t3)), obj$task_position(t3) - 1)
})


test_that("task_overview", {
  obj <- test_rrq("myfuns.R")

  expect_equal(
    obj$task_overview(),
    list(PENDING = 0, RUNNING = 0, COMPLETE = 0, ERROR = 0, CANCELLED = 0,
         DIED = 0, TIMEOUT = 0, IMPOSSIBLE = 0, DEFERRED = 0))

  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(1))
  t3 <- obj$enqueue(sin(1))

  expect_equal(
    obj$task_overview(),
    list(PENDING = 3, RUNNING = 0, COMPLETE = 0, ERROR = 0, CANCELLED = 0,
         DIED = 0, TIMEOUT = 0, IMPOSSIBLE = 0, DEFERRED = 0))
})


test_that("wait for tasks", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj)

  t1 <- obj$enqueue(1 + 1)
  t2 <- obj$enqueue(2 + 2)

  res <- obj$tasks_wait(c(t1, t2))
  expect_equal(res, set_names(list(2, 4), c(t1, t2)))

  ## Slightly slower jobs:
  t3 <- obj$enqueue(slowdouble(0.1))
  t4 <- obj$enqueue(slowdouble(0.1))
  res <- obj$tasks_wait(c(t3, t4))
  expect_equal(res, set_names(list(0.2, 0.2), c(t3, t4)))

  expect_error(
    obj$tasks_wait(c(t1, t2), time_poll = 0.1),
    "time_poll must be integer like")
  expect_error(
    obj$tasks_wait(c(t1, t2), time_poll = -1),
    "time_poll cannot be less than 1")
})


test_that("task delete", {
  obj <- test_rrq("myfuns.R")
  t1 <- obj$enqueue(1 + 1)
  t2 <- obj$enqueue(2 + 2)
  t3 <- obj$enqueue(3 + 3)

  expect_setequal(obj$task_list(), c(t1, t2, t3))
  obj$task_delete(t1)
  expect_setequal(obj$task_list(), c(t2, t3))
  obj$task_delete(c(t2, t3))
  expect_setequal(obj$task_list(), character(0))
})


test_that("stop worker", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  obj$worker_stop()
  expect_error(
    w$step(), "BYE", class = "rrq_worker_stop")
})


test_that("Can't read logs unless enabled", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  expect_error(
    obj$worker_process_log(w$name),
    "Process log not enabled for this worker")
})


test_that("Can't delete running tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  id <- obj$enqueue(sin(1))
  w$poll(TRUE)
  worker_run_task_start(w, r6_private(w), id)
  expect_error(
    obj$task_delete(id),
    "Can't delete running tasks")
})


test_that("Error if results are not ready", {
  obj <- test_rrq()
  id1 <- obj$enqueue(sin(1))
  id2 <- obj$enqueue(sin(1))
  expect_error(obj$task_result(id1),
               sprintf("Missing result for task: '%s'", id1))
  expect_error(obj$tasks_result(id1),
               sprintf("Missing result for task:\n  - %s", id1))
  expect_error(obj$tasks_result(c(id1, id2)),
               sprintf("Missing result for task:\n  - %s\n  - %s", id1, id2))
})


test_that("worker load", {
  obj <- test_rrq()
  w1 <- test_worker_blocking(obj)
  w2 <- test_worker_blocking(obj)
  load <- obj$worker_load()
  expect_s3_class(load, "worker_load")
  expect_setequal(load$worker_id, c(w1$name, w2$name))
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
  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(1))
  t3 <- obj$enqueue(sin(1))

  expect_equal(obj$queue_length(), 3)
  expect_equal(obj$queue_remove(t2), TRUE)
  expect_equal(obj$queue_length(), 2)
  expect_equal(obj$queue_list(), c(t1, t3))

  expect_equal(obj$queue_remove(c(t1, t2, t3)), c(TRUE, FALSE, TRUE))
  expect_equal(obj$queue_remove(character(0)), logical(0))
})


test_that("cancel queued task", {
  obj <- test_rrq()
  t <- obj$enqueue(sqrt(1))
  expect_equal(obj$task_status(t), set_names(TASK_PENDING, t))
  obj$task_cancel(t)
  expect_equal(obj$task_status(t), set_names(TASK_MISSING, t))
  expect_equal(obj$queue_list(), character(0))
})


test_that("cancel queued task from alternative queue", {
  obj <- test_rrq()
  t1 <- obj$enqueue(sqrt(1), queue = "other")
  t2 <- obj$enqueue(sqrt(1), queue = "other")
  t3 <- obj$enqueue(sqrt(1), queue = "other")
  expect_equal(obj$queue_list("other"), c(t1, t2, t3))
  obj$task_cancel(t2)
  expect_equal(obj$queue_list("other"), c(t1, t3))
  expect_equal(obj$task_status(t2), set_names(TASK_MISSING, t2))
})


test_that("can't cancel completed task", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t <- obj$enqueue(sqrt(1))
  w$step(TRUE)
  expect_error(
    obj$task_cancel(t),
    "Task [[:xdigit:]]{32} is not cancelable \\(COMPLETE\\)")
})


test_that("can't cancel nonexistant task", {
  obj <- test_rrq()
  id <- ids::random_id()
  expect_error(
    obj$task_cancel(id),
    "Task [[:xdigit:]]{32} is not cancelable \\(MISSING\\)")
})


test_that("can't cancel running in-process task", {
  obj <- test_rrq(timeout_worker_stop = 0)
  w <- test_worker_spawn(obj)
  t <- obj$enqueue(Sys.sleep(20))
  wait_status(t, obj)
  expect_error(
    obj$task_cancel(t, wait = TRUE, delete = FALSE),
    "Can't cancel running task '[[:xdigit:]]{32}' as not in separate process")
  obj$worker_stop(w, "kill_local")
})


test_that("get task info", {
  obj <- test_rrq()

  a <- 1
  b <- 2
  t <- obj$enqueue(log(a, b))
  res <- obj$task_data(t)
  expect_equal(res$expr, quote(log(a, b)))
  expect_mapequal(res$objects, list(a = 1, b = 2))
})


test_that("get task data errors appropriately if task is missing", {
  obj <- test_rrq()
  id <- ids::random_id()
  expect_error(
    obj$task_data(id),
    "Task '[[:xdigit:]]+' not found")
})


test_that("a worker will pick up tasks from the priority queue", {
  obj <- test_rrq("myfuns.R")
  obj$worker_config_save("localhost", queue = c("a", "b"), verbose = FALSE)
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(2), queue = "b")
  t3 <- obj$enqueue(sin(3), queue = "a")

  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               rep("PENDING", 3))
  expect_equal(obj$queue_list(), t1)
  expect_equal(obj$queue_list("b"), t2)
  expect_equal(obj$queue_list("a"), t3)

  w$step(TRUE)
  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               c("PENDING", "PENDING", "COMPLETE"))
  w$step(TRUE)
  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               c("PENDING", "COMPLETE", "COMPLETE"))
  w$step(TRUE)
  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               c("COMPLETE", "COMPLETE", "COMPLETE"))
})


test_that("Query jobs in different queues", {
  obj <- test_rrq("myfuns.R")

  t1 <- obj$enqueue(sin(1), queue = "a")
  t2 <- obj$enqueue(sin(2), queue = "a")
  t3 <- obj$enqueue(sin(3), queue = "a")

  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               rep("PENDING", 3))
  expect_equal(obj$queue_list(), character(0))
  expect_equal(obj$queue_list("a"), c(t1, t2, t3))

  expect_equal(obj$task_position(t1, queue = "a"), 1)
  expect_equal(obj$task_position(t2, queue = "a"), 2)

  expect_true(obj$queue_remove(t1, queue = "a"))
  expect_false(obj$queue_remove(t1, queue = "a"))

  expect_equal(obj$task_position(t2, queue = "a"), 1)
  expect_equal(obj$queue_length("a"), 2)
  expect_equal(obj$queue_list("a"), c(t2, t3))
})


test_that("Send job to new process", {
  skip_if_not_installed("callr")
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(slowdouble(0.1), separate_process = TRUE)
  expect_type(t, "character")
  w$step(TRUE)
  expect_equal(obj$task_wait(t, 2), 0.2)
  expect_equal(obj$task_result(t), 0.2)
})


## Doing this version does require that we spawn a worker because we
## need to interrupt the secondary worker loop. However, we could do
## an in-process version by being a bit sneaky and writing a job that
## will cancel itself.
test_that("Cancel job sent to new process", {
  skip_if_not_installed("callr")
  obj <- test_rrq("myfuns.R")

  w <- test_worker_spawn(obj)

  t <- obj$enqueue(slowdouble(50), separate_process = TRUE)
  wait_status(t, obj)
  obj$task_cancel(t, wait = TRUE, delete = FALSE)
  st <- obj$task_status(t)
  log <- obj$worker_log_tail(w, Inf)
  expect_equal(log$command,
               c("ALIVE", "TASK_START", "REMOTE", "CANCEL", "TASK_CANCELLED"))
  expect_equal(obj$task_status(t), set_names(TASK_CANCELLED, t))
  expect_equal(obj$task_result(t),
               worker_task_failed(TASK_CANCELLED, obj$queue_id, t))
})


test_that("Delete job after cancellation", {
  skip_if_not_installed("callr")
  obj <- test_rrq("myfuns.R")

  w <- test_worker_spawn(obj)

  t <- obj$enqueue(slowdouble(50), separate_process = TRUE)
  wait_status(t, obj)
  obj$task_cancel(t, wait = TRUE, delete = TRUE)
  st <- obj$task_status(t)
  log <- obj$worker_log_tail(w, Inf)
  expect_equal(log$command,
               c("ALIVE", "TASK_START", "REMOTE", "CANCEL", "TASK_CANCELLED"))
  expect_equal(obj$task_status(t), set_names(TASK_MISSING, t))
})


test_that("can check task exists", {
  obj <- test_rrq("myfuns.R")
  t <- obj$enqueue(sin(0))
  t2 <- obj$enqueue(sin(0))
  expect_true(obj$task_exists(t))
  expect_false(obj$task_exists("123"))
  expect_equal(obj$task_exists(c(t, t2)),
               setNames(c(TRUE, TRUE), c(t, t2)))
  expect_equal(obj$task_exists(c(t, "123")),
               setNames(c(TRUE, FALSE), c(t, "123")))
})


test_that("queue task with missing dependency throws an error", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  expect_error(obj$enqueue(sin(0), depends_on = "123"),
               "Failed to queue as dependency 123 does not exist.")
  expect_error(obj$enqueue(sin(0), depends_on = c("123", "456")),
               "Failed to queue as dependencies 123, 456 do not exist.")
})


test_that("task can be queued with dependency", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(sin(0))
  t2 <- obj$enqueue(sin(0))
  expect_equal(obj$queue_list(), c(t, t2))
  t3 <- obj$enqueue(sin(pi / 2), depends_on = c(t, t2))
  ## t3 has not been added to main queue yet
  expect_equal(obj$queue_list(), c(t, t2))
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")

  ## Original dependencies are stored
  original_deps_keys <- rrq_key_task_dependencies_original(
    obj$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(original_deps_keys), c(t, t2))

  ## Pending dependencies are stored
  dependency_keys <- rrq_key_task_dependencies(obj$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(dependency_keys), c(t, t2))

  ## Inverse depends_on relationship is stored
  dependent_keys <- rrq_key_task_dependents(obj$queue_id, c(t, t2))
  for (key in dependent_keys) {
    expect_equal(obj$con$SMEMBERS(key), list(t3))
  }

  ## t3 is in deferred set
  deferred_set <- queue_keys(obj)$deferred_set
  expect_equal(obj$con$SMEMBERS(deferred_set), list(t3))

  ## Function to retrieve status of t3 and see what it is waiting for

  w$step(TRUE)
  obj$task_wait(t, 2)
  expect_equal(unname(obj$task_status(t)), "COMPLETE")
  expect_equal(unname(obj$task_status(t2)), "PENDING")
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")
  ## Still not on queue
  expect_equal(obj$queue_list(), t2)
  ## Status of it has updated
  expect_setequal(obj$con$SMEMBERS(original_deps_keys), c(t, t2))
  expect_equal(obj$con$SMEMBERS(dependency_keys), list(t2))
  expect_equal(obj$con$SMEMBERS(deferred_set), list(t3))

  w$step(TRUE)
  obj$task_wait(t2, 2)
  expect_equal(unname(obj$task_status(t2)), "COMPLETE")
  expect_equal(unname(obj$task_status(t3)), "PENDING")
  ## Now added to queue
  expect_equal(obj$queue_list(), t3)
  ## Status can be retrieved
  expect_setequal(obj$con$SMEMBERS(original_deps_keys), c(t, t2))
  expect_equal(obj$con$SMEMBERS(dependency_keys), list())
  expect_equal(obj$con$SMEMBERS(deferred_set), list())

  w$step(TRUE)
  obj$task_wait(t3, 2)
  expect_equal(unname(obj$task_status(t3)), "COMPLETE")
})


test_that("task added to queue immediately if dependencies satified", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(sin(0))
  expect_equal(obj$queue_list(), t)

  w$step(TRUE)
  obj$task_wait(t, 2)

  ## Immediately added to queue as t has completed
  t2 <- obj$enqueue(sin(pi / 2), depends_on = t)
  expect_equal(obj$queue_list(), t2)
  expect_equal(unname(obj$task_status(t2)), TASK_PENDING)
})


test_that("queueing with depends_on errored task fails", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(only_positive(-1))
  w$step(TRUE)
  res <- obj$task_result(t)
  expect_s3_class(res, "rrq_task_error")

  expect_error(obj$enqueue(sin(0), depends_on = t),
               paste0("Failed to queue as dependent tasks failed:\n",
                      t, ": ERROR"),
               fixed = TRUE)

  ## deferred set is empty
  deferred_set <- queue_keys(obj)$deferred_set
  expect_equal(obj$con$SMEMBERS(deferred_set), list())

  ## Task is set to impossible
  status <- obj$task_status()
  expect_length(status, 2)
  expect_true(TASK_IMPOSSIBLE %in% status)
  expect_true(TASK_ERROR %in% status)
})


test_that("dependent tasks updated if dependency fails", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(only_positive(-1))
  t2 <- obj$enqueue(sin(0), depends_on = t)
  t3 <- obj$enqueue(sin(0), depends_on = t)
  t4 <- obj$enqueue(sin(0), depends_on = t2)

  expect_equal(obj$queue_list(), t)
  expect_equal(unname(obj$task_status(t)), "PENDING")
  expect_equal(unname(obj$task_status(t2)), "DEFERRED")
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")
  expect_equal(unname(obj$task_status(t4)), "DEFERRED")

  w$step(TRUE)
  obj$task_wait(t, 2)
  expect_equal(unname(obj$task_status(t)), "ERROR")

  ## Dependent task updated and nothing queued
  expect_equal(unname(obj$task_status(t2)), "IMPOSSIBLE")
  expect_equal(unname(obj$task_status(t3)), "IMPOSSIBLE")
  expect_equal(unname(obj$task_status(t4)), "IMPOSSIBLE")
  expect_equal(obj$queue_list(), character(0))
  deferred_set <- queue_keys(obj)$deferred_set
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
})


test_that("multiple tasks can be queued with same dependency", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(sin(0))
  t2 <- obj$enqueue(sin(0), depends_on = t)
  t3 <- obj$enqueue(sin(0), depends_on = t)
  expect_equal(obj$queue_list(), t)
  expect_equal(unname(obj$task_status(t2)), "DEFERRED")
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")

  ## t3 is in deferred set
  deferred_set <- queue_keys(obj)$deferred_set
  expect_setequal(obj$con$SMEMBERS(deferred_set), c(t2, t3))

  w$step(TRUE)
  obj$task_wait(t, 2)
  expect_equal(unname(obj$task_status(t)), "COMPLETE")
  expect_equal(unname(obj$task_status(t2)), "PENDING")
  expect_equal(unname(obj$task_status(t3)), "PENDING")
  expect_setequal(obj$queue_list(), c(t2, t3))

  expect_equal(obj$con$SMEMBERS(deferred_set), list())
})


test_that("deferred task is added to specified queue", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(sin(0))
  t2 <- obj$enqueue(sin(0), depends_on = t)
  t3 <- obj$enqueue(sin(0), depends_on = t, queue = "a")
  expect_equal(obj$queue_list(), t)
  expect_equal(obj$queue_list("a"), character(0))
  deferred_set <- queue_keys(obj)$deferred_set
  expect_setequal(obj$con$SMEMBERS(deferred_set), list(t2, t3))

  w$step(TRUE)
  obj$task_wait(t, 2)

  expect_equal(obj$queue_list(), t2)
  expect_equal(obj$queue_list("a"), t3)
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
})


test_that("task set to impossible cannot be added to queue", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(only_positive(-1))
  t2 <- obj$enqueue(sin(0))
  t3 <- obj$enqueue(sin(pi / 2), depends_on = c(t, t2))
  expect_equal(unname(obj$task_status(t)), "PENDING")
  expect_equal(unname(obj$task_status(t2)), "PENDING")
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")
  expect_equal(obj$queue_list(), c(t, t2))
  deferred_set <- queue_keys(obj)$deferred_set
  expect_equal(obj$con$SMEMBERS(deferred_set), list(t3))

  w$step(TRUE)
  res <- obj$task_result(t)
  expect_s3_class(res, "rrq_task_error")

  expect_equal(unname(obj$task_status(t)), "ERROR")
  expect_equal(unname(obj$task_status(t2)), "PENDING")
  expect_equal(unname(obj$task_status(t3)), "IMPOSSIBLE")
  expect_equal(obj$queue_list(), t2)
  expect_equal(obj$con$SMEMBERS(deferred_set), list())

  w$step(TRUE)
  obj$task_wait(t2, 2)

  expect_equal(unname(obj$task_status(t2)), "COMPLETE")
  expect_equal(unname(obj$task_status(t3)), "IMPOSSIBLE")
  expect_equal(obj$queue_list(), character(0))
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
})


test_that("deferred task delete", {
  obj <- test_rrq("myfuns.R")
  t1 <- obj$enqueue(1 + 1)
  t2 <- obj$enqueue(2 + 2, depends_on = t1)
  t3 <- obj$enqueue(2 + 2, depends_on = t1)
  t4 <- obj$enqueue(2 + 2, depends_on = t3)

  expect_setequal(obj$task_list(), c(t1, t2, t3, t4))
  expect_equal(obj$queue_list(), t1)
  deferred_set <- queue_keys(obj)$deferred_set
  expect_setequal(obj$con$SMEMBERS(deferred_set), list(t2, t3, t4))

  obj$task_delete(t2)
  expect_setequal(obj$task_list(), c(t1, t3, t4))
  expect_equal(obj$queue_list(), t1)
  expect_setequal(obj$con$SMEMBERS(deferred_set), list(t3, t4))
  expect_equal(unname(obj$task_status(t1)), "PENDING")
  expect_equal(unname(obj$task_status(t2)), "MISSING")
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")
  expect_equal(unname(obj$task_status(t4)), "DEFERRED")

  obj$task_delete(t1)
  expect_setequal(obj$task_list(), c(t3, t4))
  expect_setequal(obj$queue_list(), character(0))
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
  expect_equal(unname(obj$task_status(t1)), "MISSING")
  expect_equal(unname(obj$task_status(t2)), "MISSING")
  expect_equal(unname(obj$task_status(t3)), "IMPOSSIBLE")
  expect_equal(unname(obj$task_status(t4)), "IMPOSSIBLE")

  obj$task_delete(t3)
  expect_setequal(obj$task_list(), t4)
  expect_setequal(obj$queue_list(), character(0))
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
  expect_equal(unname(obj$task_status(t1)), "MISSING")
  expect_equal(unname(obj$task_status(t2)), "MISSING")
  expect_equal(unname(obj$task_status(t3)), "MISSING")
  expect_equal(unname(obj$task_status(t4)), "IMPOSSIBLE")
})


test_that("delete completed task does not clear dependencies", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  t1 <- obj$enqueue(1 + 1)
  t2 <- obj$enqueue(2 + 2, depends_on = t1)

  w$step(TRUE)
  obj$task_wait(t1, 2)

  expect_setequal(obj$task_list(), c(t1, t2))
  expect_equal(obj$queue_list(), t2)
  expect_equal(unname(obj$task_status(t1)), "COMPLETE")
  expect_equal(unname(obj$task_status(t2)), "PENDING")

  obj$task_delete(t1)
  expect_setequal(obj$task_list(), t2)
  expect_equal(obj$queue_list(), t2)
  expect_equal(unname(obj$task_status(t1)), "MISSING")
  expect_equal(unname(obj$task_status(t2)), "PENDING")
})


test_that("deferred task cancel", {
  obj <- test_rrq("myfuns.R")
  t1 <- obj$enqueue(1 + 1)
  t2 <- obj$enqueue(2 + 2, depends_on = t1)
  t3 <- obj$enqueue(2 + 2, depends_on = t1)
  t4 <- obj$enqueue(2 + 2, depends_on = t3)

  expect_setequal(obj$task_list(), c(t1, t2, t3, t4))
  expect_equal(obj$queue_list(), t1)
  deferred_set <- queue_keys(obj)$deferred_set
  expect_setequal(obj$con$SMEMBERS(deferred_set), list(t2, t3, t4))

  obj$task_cancel(t2)
  expect_setequal(obj$task_list(), c(t1, t3, t4))
  expect_equal(obj$queue_list(), t1)
  expect_setequal(obj$con$SMEMBERS(deferred_set), list(t3, t4))
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")
  expect_equal(unname(obj$task_status(t4)), "DEFERRED")

  obj$task_cancel(t1)
  expect_setequal(obj$task_list(), c(t3, t4))
  expect_setequal(obj$queue_list(), character(0))
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
  expect_equal(unname(obj$task_status(t3)), "IMPOSSIBLE")
  expect_equal(unname(obj$task_status(t4)), "IMPOSSIBLE")
})

test_that("can get deferred tasks", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(1 + 1)
  t2 <- obj$enqueue(1 + 1)
  t3 <- obj$enqueue(2 + 2, depends_on = c(t1, t2))
  t4 <- obj$enqueue(2 + 2, depends_on = t1)

  tasks <- obj$deferred_list()
  expect_setequal(names(tasks), c(t3, t4))
  expect_setequal(names(tasks[[t3]]), c(t1, t2))
  expect_equal(tasks[[t3]][[t1]], "PENDING")
  expect_equal(tasks[[t3]][[t2]], "PENDING")
  expect_equal(names(tasks[[t4]]), t1)
  expect_equal(tasks[[t4]][[t1]], "PENDING")

  w$step(TRUE)
  obj$task_wait(t1, 2)

  tasks <- obj$deferred_list()
  expect_equal(names(tasks), t3)
  expect_setequal(names(tasks[[t3]]), c(t1, t2))
  expect_equal(tasks[[t3]][[t1]], "COMPLETE")
  expect_equal(tasks[[t3]][[t2]], "PENDING")

  w$step(TRUE)
  w$step(TRUE)
  obj$task_wait(t2, 2)
  obj$task_wait(t4, 2)

  tasks <- obj$deferred_list()
  expect_setequal(obj$deferred_list(), list())
})


test_that("can use task_wait with impossible tasks", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(only_positive(-1))
  t2 <- obj$enqueue(sin(0), depends_on = t)

  expect_equal(obj$queue_list(), t)
  expect_equal(unname(obj$task_status(t)), "PENDING")
  expect_equal(unname(obj$task_status(t2)), "DEFERRED")

  w$step(TRUE)
  out <- obj$task_wait(t, 2)
  expect_equal(unname(obj$task_status(t)), "ERROR")
  expect_equal(out$task_id, t)
  expect_equal(out$queue_id, obj$queue_id)

  ## task wait returns for an impossible task
  expect_equal(unname(obj$task_status(t2)), "IMPOSSIBLE")
  out <- obj$task_wait(t2, 2)
  ## Not just a timeout error - it has returned
  expect_s3_class(out, "rrq_task_error")
  expect_equal(out$message, "Task not successful: IMPOSSIBLE")
  expect_equal(out$task_id, t2)
  expect_equal(out$queue_id, obj$queue_id)
})


test_that("submit a task with a timeout requires separate process", {
  obj <- test_rrq("myfuns.R")
  expect_error(
    obj$enqueue(slowdouble(10), timeout_task_run = 1),
    "Can't set timeout as 'separate_process' is FALSE")
})


test_that("submit a task with a timeout", {
  obj <- test_rrq("myfuns.R")
  t <- obj$enqueue(slowdouble(10), timeout_task_run = 1,
                   separate_process = TRUE)
  expect_equal(obj$con$HGET(queue_keys(obj)$task_timeout, t), "1")

  w <- test_worker_blocking(obj)
  w$step(TRUE)

  expect_equal(obj$task_status(t), set_names(TASK_TIMEOUT, t))
  expect_equal(obj$task_result(t),
               worker_task_failed(TASK_TIMEOUT, obj$queue_id, t))

  expect_equal(obj$worker_log_tail(w$name, Inf)$command,
               c("ALIVE", "TASK_START", "REMOTE", "TIMEOUT", "TASK_TIMEOUT"))
})


test_that("manual control over environment", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  e <- new.env(parent = environment())
  e$a <- 10

  t <- obj$enqueue(sqrt(a), e, export = character(0))
  w$step(TRUE)
  expect_equal(obj$task_status(t), set_names(TASK_ERROR, t))
  expect_length(obj$task_data(t)$objects, 0)

  w_envir <- r6_private(w)$envir
  w_envir$a <- 2
  t <- obj$enqueue(sqrt(a), e, export = character(0))
  w$step(TRUE)
  expect_equal(obj$task_status(t), set_names(TASK_COMPLETE, t))
  expect_length(obj$task_data(t)$objects, 0)
  expect_equal(obj$task_result(t), sqrt(2))

  t <- obj$enqueue(sqrt(a), e, export = "a")
  w$step(TRUE)
  expect_equal(obj$task_status(t), set_names(TASK_COMPLETE, t))
  expect_length(obj$task_data(t)$objects, 1)
  expect_equal(obj$task_result(t), sqrt(10))
})


test_that("can offload storage", {
  skip_if_no_redis()
  name <- sprintf("rrq:%s", ids::random_id())

  path <- tempfile()

  obj <- test_rrq(store_max_size = 100, offload_path = path)
  a <- 10
  b <- runif(20)
  t <- obj$enqueue(sum(b) / a)

  w <- test_worker_blocking(obj)
  w$step(TRUE)

  expect_equal(obj$task_result(t), sum(b) / a)

  ## Did successfully offload data:
  store <- r6_private(obj)$store
  h <- store$list()
  expect_length(h, 3)
  expect_setequal(store$location(h), c("redis", "offload"))
  expect_equal(store$tags(), t)
  expect_length(dir(path), 1L)
  expect_true(dir(path) %in% h)

  obj$task_delete(t)

  expect_equal(store$list(), character(0))
  expect_equal(dir(path), character(0))
})


test_that("offload storage in result", {
  path <- tempfile()
  obj <- test_rrq(store_max_size = 100, offload_path = path)
  t <- obj$enqueue(rep(1, 100))

  w <- test_worker_blocking(obj)
  w$step(TRUE)

  expect_equal(obj$task_result(t), rep(1, 100))

  ## Did successfully offload data:
  store <- r6_private(obj)$store
  h <- store$list()
  expect_length(h, 1)
  expect_setequal(store$location(h), "offload")
  expect_equal(store$tags(), t)
  expect_equal(dir(path), h)

  obj$task_delete(t)

  expect_equal(store$list(), character(0))
  expect_equal(dir(path), character(0))
})


test_that("collect times", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(slowdouble(0.1))
  t2 <- obj$enqueue(slowdouble(0.1))
  tt <- c(t1, t2)

  times1 <- obj$task_times(tt)
  expect_true(is.matrix(times1)) # testthat 3e makes this quite hard
  expect_equal(dimnames(times1), list(tt, c("submit", "start", "complete")))
  expect_type(times1, "double")
  expect_equal(times1[tt, "start"], set_names(rep(NA_real_, 2), tt))
  expect_equal(times1[tt, "complete"], set_names(rep(NA_real_, 2), tt))
  expect_false(any(is.na(times1[tt, "submit"])))

  w$step(TRUE)
  times2 <- obj$task_times(tt)
  expect_equal(times2[t2, , drop = FALSE], times1[t2, , drop = FALSE])
  expect_equal(obj$task_times(t2), times1[t2, , drop = FALSE])
  expect_false(any(is.na(times2[t1, ])))
})


test_that("task errors can be immediately thrown", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(only_positive(-1))
  w$step(TRUE)
  err <- expect_error(obj$task_result(t, error = TRUE),
                      class = "rrq_task_error")
  expect_equal(err$queue_id, obj$queue_id)
  expect_equal(err$task_id, t)

  err2 <- expect_error(obj$task_wait(t, error = TRUE),
                       class = "rrq_task_error")
  expect_equal(err2, err)

  err3 <- expect_error(obj$tasks_result(c(t, t), error = TRUE),
                       class = "rrq_task_error_group")
  expect_equal(err3$errors, list(err, err))

  expect_equal(
    format(err3),
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
  t <- obj$enqueue(sqrt(4))

  d1 <- obj$task_info(t)
  expect_setequal(
    names(d1),
    c("id", "status", "queue", "separate_process", "timeout", "worker", "pid"))
  expect_equal(d1$id, t)
  expect_equal(d1$status, TASK_PENDING)
  expect_equal(d1$queue, "default")
  expect_false(d1$separate_process)
  expect_null(d1$timeout)
  expect_null(d1$worker)
  expect_null(d1$pid)

  w$step(TRUE)
  d2 <- obj$task_info(t)
  expect_setequal(names(d2), names(d1))
  expect_equal(d2$id, t)
  expect_equal(d2$status, TASK_COMPLETE)
  expect_equal(d2$queue, "default")
  expect_false(d2$separate_process)
  expect_null(d2$timeout, 5)
  expect_equal(d2$worker, w$name)
  expect_null(d2$pid, "integer")
})


test_that("Can get information about a task in a different process", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t <- obj$enqueue(sqrt(4), separate_process = TRUE, timeout_task_run = 5)

  d1 <- obj$task_info(t)
  expect_setequal(
    names(d1),
    c("id", "status", "queue", "separate_process", "timeout", "worker", "pid"))
  expect_equal(d1$id, t)
  expect_equal(d1$status, TASK_PENDING)
  expect_equal(d1$queue, "default")
  expect_true(d1$separate_process)
  expect_equal(d1$timeout, 5)
  expect_null(d1$worker)
  expect_null(d1$pid)

  w$step(TRUE)
  d2 <- obj$task_info(t)
  expect_setequal(names(d2), names(d1))
  expect_equal(d2$id, t)
  expect_equal(d2$status, TASK_COMPLETE)
  expect_equal(d2$queue, "default")
  expect_true(d2$separate_process)
  expect_equal(d2$timeout, 5)
  expect_equal(d2$worker, w$name)
  expect_type(d2$pid, "integer")
})


test_that("Can get information about task retries", {
  obj <- test_rrq()
  t <- list()
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(identity(1))
  w$step(TRUE)
  expect_mapequal(obj$task_info(t1)$moved, list(up = NULL, down = NULL))

  t2 <- obj$task_retry(t1)
  w$step(TRUE)
  expect_mapequal(obj$task_info(t1)$moved, list(up = NULL, down = t2))
  expect_mapequal(obj$task_info(t2)$moved, list(up = t1, down = NULL))

  t3 <- obj$task_retry(t2)
  w$step(TRUE)
  expect_mapequal(obj$task_info(t1)$moved, list(up = NULL, down = c(t2, t3)))
  expect_mapequal(obj$task_info(t2)$moved, list(up = t1, down = t3))
  expect_mapequal(obj$task_info(t3)$moved, list(up = c(t1, t2), down = NULL))

  t4 <- obj$task_retry(t3)
  w$step(TRUE)
  expect_mapequal(obj$task_info(t1)$moved,
                  list(up = NULL, down = c(t2, t3, t4)))
  expect_mapequal(obj$task_info(t2)$moved,
                  list(up = t1, down = c(t3, t4)))
  expect_mapequal(obj$task_info(t3)$moved,
                  list(up = c(t1, t2), down = t4))
  expect_mapequal(obj$task_info(t4)$moved,
                  list(up = c(t1, t2, t3), down = NULL))
})
