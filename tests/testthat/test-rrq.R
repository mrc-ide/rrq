context("rrq")

test_that("empty", {
  obj <- test_rrq()

  expect_is(obj, "rrq_controller")

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
  expect_is(t, "character")
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
  expect_is(res, "rrq_task_error")
  expect_null(res$warnings)

  t3 <- obj$enqueue(nonexistant_function(-1))
  w$step(TRUE)
  res <- obj$task_result(t3)
  expect_is(res, "rrq_task_error")
  expect_null(res$warnings)
})


test_that("task warnings are returned", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(warning_then_error(2))
  expect_warning(
    w$step(TRUE),
    "This is warning number \\d")

  r1 <- obj$task_result(t1)
  expect_is(r1, "rrq_task_error")
  expect_is(r1$warnings, "character")
  expect_equal(length(r1$warnings), 2)
  expect_equal(r1$warnings, sprintf("This is warning number %d", 1:2))

  expect_match(r1$trace, "^warning_then_error", all = FALSE)
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
    list(PENDING = 0, RUNNING = 0, COMPLETE = 0, ERROR = 0))

  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(1))
  t3 <- obj$enqueue(sin(1))

  expect_equal(
    obj$task_overview(),
    list(PENDING = 3, RUNNING = 0, COMPLETE = 0, ERROR = 0))
})


test_that("wait for tasks without key", {
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
})


test_that("wait for tasks with key", {
  obj <- test_rrq("myfuns.R")
  k1 <- rrq_key_task_complete(obj$queue_id)
  t1 <- obj$enqueue(1 + 1, key_complete = k1)
  t2 <- obj$enqueue(2 + 2, key_complete = k1)

  wid <- test_worker_spawn(obj)

  expect_error(
    obj$tasks_wait(c(t1, t2), key_complete = k1, time_poll = 0.1),
    "time_poll must be integer like")
  expect_error(
    obj$tasks_wait(c(t1, t2), key_complete = k1, time_poll = -1),
    "time_poll cannot be less than 1 if using key_complete")
  res <- obj$tasks_wait(c(t1, t2), key_complete = k1)
  expect_equal(res, set_names(list(2, 4), c(t1, t2)))

  ## Slightly slower jobs:
  k2 <- rrq_key_task_complete(obj$queue_id)
  t3 <- obj$enqueue(slowdouble(0.1), key_complete = k2)
  t4 <- obj$enqueue(slowdouble(0.1), key_complete = k2)
  res <- obj$tasks_wait(c(t3, t4), key_complete = k2)
  expect_equal(res, set_names(list(0.2, 0.2), c(t3, t4)))
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


test_that("wait for tasks on a key", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj)
  key <- rrq_key_task_complete(obj$queue_id)

  id <- obj$enqueue(sin(1), key_complete = key)
  res <- obj$task_wait(id, 1, key_complete = NULL, progress = FALSE)
  expect_equal(obj$con$EXISTS(key), 1)
  expect_equal(obj$task_wait(id, key_complete = key), sin(1))
  expect_equal(obj$con$EXISTS(key), 0)
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
  worker_run_task_start(w, id)
  expect_error(
    obj$task_delete(id),
    "Can't delete running tasks")
})


test_that("Error if results are not ready", {
  obj <- test_rrq()
  id <- obj$enqueue(sin(1))
  expect_error(obj$task_result(id), "Missing some results")
})


test_that("worker load", {
  obj <- test_rrq()
  w1 <- test_worker_blocking(obj)
  w2 <- test_worker_blocking(obj)
  load <- obj$worker_load()
  expect_is(load, "worker_load")
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
  expect_equal(w$envir$x, 1)

  obj$envir(NULL)
  expect_message(w$step(TRUE), "REFRESH")
  expect_equal(ls(w$envir), character(0))
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


test_that("worker_send_signal", {
  obj <- test_rrq()
  w1 <- test_worker_blocking(obj)
  w2 <- test_worker_blocking(obj)
  wid <- c(w1$name, w2$name)
  worker_send_signal(obj$con, obj$keys, tools::SIGINT, c(w1$name, w2$name))

  k <- paste0(rrq_key_worker_heartbeat(obj$keys$queue_id, wid), ":signal")
  expect_equal(obj$con$LINDEX(k[[1]], 0), as.character(tools::SIGINT))
  expect_equal(obj$con$LINDEX(k[[2]], 0), as.character(tools::SIGINT))
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
  obj <- test_rrq()
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
  obj$worker_config_save("localhost", queue = c("a", "b"))
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
  expect_is(t, "character")
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
               worker_task_failed(TASK_CANCELLED))
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


test_that("task can be added to front of queue", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(sin(0))
  expect_equal(obj$queue_list(), t)
  t2 <- obj$enqueue(sin(pi / 2), at_front = TRUE)
  expect_equal(obj$queue_list(), c(t2, t))

  expect_equivalent(obj$task_status(t), "PENDING")
  expect_equivalent(obj$task_status(t2), "PENDING")
  w$step(TRUE)
  expect_equal(obj$task_wait(t2, 2), 1)
  expect_equivalent(obj$task_status(t), "PENDING")
  expect_equivalent(obj$task_status(t2), "COMPLETE")
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
  expect_equivalent(obj$task_status(t3), "DEFERRED")

  ## Original dependencies are stored
  original_deps_keys <- rrq_key_task_dependencies_original(
    obj$keys$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(original_deps_keys), c(t, t2))

  ## Pending dependencies are stored
  dependency_keys <- rrq_key_task_dependencies(obj$keys$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(dependency_keys), c(t, t2))

  ## Inverse depends_on relationship is stored
  dependent_keys <- rrq_key_task_dependents(obj$keys$queue_id, c(t, t2))
  for (key in dependent_keys) {
    expect_equal(obj$con$SMEMBERS(key), list(t3))
  }

  ## t3 is in deferred set
  deferred_set <- obj$keys$deferred_set
  expect_equal(obj$con$SMEMBERS(deferred_set), list(t3))

  ## Function to retrieve status of t3 and see what it is waiting for

  w$step(TRUE)
  obj$task_wait(t, 2)
  expect_equivalent(obj$task_status(t), "COMPLETE")
  expect_equivalent(obj$task_status(t2), "PENDING")
  expect_equivalent(obj$task_status(t3), "DEFERRED")
  ## Still not on queue
  expect_equal(obj$queue_list(), t2)
  ## Status of it has updated
  expect_setequal(obj$con$SMEMBERS(original_deps_keys), c(t, t2))
  expect_equal(obj$con$SMEMBERS(dependency_keys), list(t2))
  expect_equal(obj$con$SMEMBERS(deferred_set), list(t3))

  w$step(TRUE)
  obj$task_wait(t2, 2)
  expect_equivalent(obj$task_status(t2), "COMPLETE")
  expect_equivalent(obj$task_status(t3), "PENDING")
  ## Now added to queue
  expect_equal(obj$queue_list(), t3)
  ## Status can be retrieved
  expect_setequal(obj$con$SMEMBERS(original_deps_keys), c(t, t2))
  expect_equal(obj$con$SMEMBERS(dependency_keys), list())
  expect_equal(obj$con$SMEMBERS(deferred_set), list())

  w$step(TRUE)
  obj$task_wait(t3, 2)
  expect_equivalent(obj$task_status(t3), "COMPLETE")
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
  expect_equivalent(obj$task_status(t2), TASK_PENDING)
})


test_that("queueing with depends_on errored task fails", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(only_positive(-1))
  w$step(TRUE)
  res <- obj$task_result(t)
  expect_is(res, "rrq_task_error")

  expect_error(obj$enqueue(sin(0), depends_on = t),
               paste0("Failed to queue as dependent tasks failed:\n",
                      t, ": ERROR"),
               fixed = TRUE)

  ## deferred set is empty
  deferred_set <- obj$keys$deferred_set
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
  expect_equivalent(obj$task_status(t), "PENDING")
  expect_equivalent(obj$task_status(t2), "DEFERRED")
  expect_equivalent(obj$task_status(t3), "DEFERRED")
  expect_equivalent(obj$task_status(t4), "DEFERRED")

  w$step(TRUE)
  obj$task_wait(t, 2)
  expect_equivalent(obj$task_status(t), "ERROR")

  ## Dependent task updated and nothing queued
  expect_equivalent(obj$task_status(t2), "IMPOSSIBLE")
  expect_equivalent(obj$task_status(t3), "IMPOSSIBLE")
  expect_equivalent(obj$task_status(t4), "IMPOSSIBLE")
  expect_equal(obj$queue_list(), character(0))
  deferred_set <- obj$keys$deferred_set
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
})


test_that("multiple tasks can be queued with same dependency", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(sin(0))
  t2 <- obj$enqueue(sin(0), depends_on = t)
  t3 <- obj$enqueue(sin(0), depends_on = t)
  expect_equal(obj$queue_list(), t)
  expect_equivalent(obj$task_status(t2), "DEFERRED")
  expect_equivalent(obj$task_status(t3), "DEFERRED")

  ## t3 is in deferred set
  deferred_set <- obj$keys$deferred_set
  expect_setequal(obj$con$SMEMBERS(deferred_set), c(t2, t3))

  w$step(TRUE)
  obj$task_wait(t, 2)
  expect_equivalent(obj$task_status(t), "COMPLETE")
  expect_equivalent(obj$task_status(t2), "PENDING")
  expect_equivalent(obj$task_status(t3), "PENDING")
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
  deferred_set <- obj$keys$deferred_set
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
  expect_equivalent(obj$task_status(t), "PENDING")
  expect_equivalent(obj$task_status(t2), "PENDING")
  expect_equivalent(obj$task_status(t3), "DEFERRED")
  expect_equal(obj$queue_list(), c(t, t2))
  deferred_set <- obj$keys$deferred_set
  expect_equal(obj$con$SMEMBERS(deferred_set), list(t3))

  w$step(TRUE)
  res <- obj$task_result(t)
  expect_is(res, "rrq_task_error")

  expect_equivalent(obj$task_status(t), "ERROR")
  expect_equivalent(obj$task_status(t2), "PENDING")
  expect_equivalent(obj$task_status(t3), "IMPOSSIBLE")
  expect_equal(obj$queue_list(), t2)
  expect_equal(obj$con$SMEMBERS(deferred_set), list())

  w$step(TRUE)
  obj$task_wait(t2, 2)

  expect_equivalent(obj$task_status(t2), "COMPLETE")
  expect_equivalent(obj$task_status(t3), "IMPOSSIBLE")
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
  deferred_set <- obj$keys$deferred_set
  expect_setequal(obj$con$SMEMBERS(deferred_set), list(t2, t3, t4))

  obj$task_delete(t2)
  expect_setequal(obj$task_list(), c(t1, t3, t4))
  expect_equal(obj$queue_list(), t1)
  expect_setequal(obj$con$SMEMBERS(deferred_set), list(t3, t4))
  expect_equivalent(obj$task_status(t1), "PENDING")
  expect_equivalent(obj$task_status(t2), "MISSING")
  expect_equivalent(obj$task_status(t3), "DEFERRED")
  expect_equivalent(obj$task_status(t4), "DEFERRED")

  obj$task_delete(t1)
  expect_setequal(obj$task_list(), c(t3, t4))
  expect_setequal(obj$queue_list(), character(0))
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
  expect_equivalent(obj$task_status(t1), "MISSING")
  expect_equivalent(obj$task_status(t2), "MISSING")
  expect_equivalent(obj$task_status(t3), "IMPOSSIBLE")
  expect_equivalent(obj$task_status(t4), "IMPOSSIBLE")

  obj$task_delete(t3)
  expect_setequal(obj$task_list(), t4)
  expect_setequal(obj$queue_list(), character(0))
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
  expect_equivalent(obj$task_status(t1), "MISSING")
  expect_equivalent(obj$task_status(t2), "MISSING")
  expect_equivalent(obj$task_status(t3), "MISSING")
  expect_equivalent(obj$task_status(t4), "IMPOSSIBLE")
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
  expect_equivalent(obj$task_status(t1), "COMPLETE")
  expect_equivalent(obj$task_status(t2), "PENDING")

  obj$task_delete(t1)
  expect_setequal(obj$task_list(), t2)
  expect_equal(obj$queue_list(), t2)
  expect_equivalent(obj$task_status(t1), "MISSING")
  expect_equivalent(obj$task_status(t2), "PENDING")
})


test_that("deferred task cancel", {
  obj <- test_rrq("myfuns.R")
  t1 <- obj$enqueue(1 + 1)
  t2 <- obj$enqueue(2 + 2, depends_on = t1)
  t3 <- obj$enqueue(2 + 2, depends_on = t1)
  t4 <- obj$enqueue(2 + 2, depends_on = t3)

  expect_setequal(obj$task_list(), c(t1, t2, t3, t4))
  expect_equal(obj$queue_list(), t1)
  deferred_set <- obj$keys$deferred_set
  expect_setequal(obj$con$SMEMBERS(deferred_set), list(t2, t3, t4))

  obj$task_cancel(t2)
  expect_setequal(obj$task_list(), c(t1, t3, t4))
  expect_equal(obj$queue_list(), t1)
  expect_setequal(obj$con$SMEMBERS(deferred_set), list(t3, t4))
  expect_equivalent(obj$task_status(t3), "DEFERRED")
  expect_equivalent(obj$task_status(t4), "DEFERRED")

  obj$task_cancel(t1)
  expect_setequal(obj$task_list(), c(t3, t4))
  expect_setequal(obj$queue_list(), character(0))
  expect_equal(obj$con$SMEMBERS(deferred_set), list())
  expect_equivalent(obj$task_status(t3), "IMPOSSIBLE")
  expect_equivalent(obj$task_status(t4), "IMPOSSIBLE")
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


test_that("submit a task with a timeout requires separate process", {
  obj <- test_rrq("myfuns.R")
  expect_error(
    obj$enqueue(slowdouble(10), timeout = 1),
    "Can't set timeout as 'separate_process' is FALSE")
})


test_that("submit a task with a timeout", {
  obj <- test_rrq("myfuns.R")
  t <- obj$enqueue(slowdouble(10), timeout = 1, separate_process = TRUE)
  expect_equal(obj$con$HGET(obj$keys$task_timeout, t), "1")

  w <- test_worker_blocking(obj)
  w$step(TRUE)

  expect_equal(obj$task_status(t), set_names(TASK_TIMEOUT, t))
  expect_equal(obj$task_result(t),
               worker_task_failed(TASK_TIMEOUT))

  expect_equal(obj$worker_log_tail(w$name, Inf)$command,
               c("ALIVE", "TASK_START", "REMOTE", "TIMEOUT", "TASK_TIMEOUT"))
})
