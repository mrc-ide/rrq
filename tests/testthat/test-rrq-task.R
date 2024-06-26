test_that("can wait on a completed task", {
  obj <- test_rrq()
  t <- rrq_task_create_expr(sqrt(2), controller = obj)
  w <- test_worker_blocking(obj)
  w$step(TRUE)

  mock_logwatch <- mockery::mock()
  mockery::stub(rrq_task_wait, "logwatch::logwatch", mock_logwatch)

  expect_true(rrq_task_wait(t, controller = obj))
  expect_equal(rrq_task_result(t, controller = obj), sqrt(2))
  mockery::expect_called(mock_logwatch, 0)
})


test_that("can error if tasks don't complete on time", {
  obj <- test_rrq()
  t <- rrq_task_create_expr(sqrt(2), controller = obj)
  new_redis <- obj$con$version() >= numeric_version("6.0.0")
  time_poll <- if (new_redis) 0.01 else 1
  expect_error(
    rrq_task_wait(t, timeout = 0, time_poll = time_poll, controller = obj),
    "task did not complete in time")
})


test_that("pass through to logwatch if waiting on tasks", {
  obj <- test_rrq()
  t <- c(rrq_task_create_expr(sqrt(1), controller = obj),
         rrq_task_create_expr(sqrt(2), controller = obj),
         rrq_task_create_expr(sqrt(3), controller = obj))

  mock_logwatch <- mockery::mock(
    list(status = c(TASK_COMPLETE, TASK_COMPLETE, TASK_COMPLETE)))
  mockery::stub(rrq_task_wait, "logwatch::logwatch", mock_logwatch)

  expect_true(
    rrq_task_wait(t, timeout = 10, time_poll = 3, progress = TRUE,
                  controller = obj))
  mockery::expect_called(mock_logwatch, 1)
  args <- mockery::mock_args(mock_logwatch)[[1]]
  expect_type(args$get_status, "closure")
  args$get_status <- NULL # can't do this comparison
  expect_equal(
    args,
    list("tasks",
         get_log = NULL,
         show_log = FALSE,
         multiple = TRUE,
         show_spinner = TRUE,
         poll = 0,
         timeout = 10,
         status_waiting = c("PENDING", "DEFERRED"),
         status_running = "RUNNING",
         status_timeout = "wait:timeout",
         status_interrupt = "wait:interrupt"))
})


test_that("fail early if we can't wait", {
  obj <- test_rrq()
  t <- c(rrq_task_create_expr(sqrt(1), controller = obj),
         rrq_task_create_expr(sqrt(2), controller = obj),
         rrq_task_create_expr(sqrt(3), controller = obj))
  expect_error(rrq_task_wait(character(), controller = obj),
               "Can't wait on no tasks")
  expect_error(rrq_task_wait(ids::random_id(), controller = obj),
               "Can't wait on missing tasks")
  expect_error(rrq_task_wait(c(t, ids::random_id()), controller = obj),
               "Can't wait on missing tasks")
  expect_error(rrq_task_wait(c(t, ids::random_id(2)), controller = obj),
               "Can't wait on missing tasks")
})


test_that("can wait for tasks", {
  mock_hmget <- mockery::mock(
    c(TASK_RUNNING, TASK_RUNNING, TASK_RUNNING))
  mock_del <- mockery::mock()
  mock_pipeline <- mockery::mock(
    list(NULL, list(TASK_RUNNING, TASK_RUNNING, TASK_RUNNING)),
    list(NULL, list(TASK_RUNNING, TASK_COMPLETE, TASK_RUNNING)),
    list(NULL, list(TASK_RUNNING, TASK_COMPLETE)),
    list(NULL, list(TASK_RUNNING)),
    list(NULL, list(TASK_COMPLETE)))

  obj <- test_rrq()
  obj$con <- list(version = mockery::mock(numeric_version("6.0.0")),
                  pipeline = mock_pipeline,
                  HMGET = mock_hmget,
                  DEL = mock_del)
  t <- ids::random_id(3)
  expect_true(rrq_task_wait(t, follow = FALSE, controller = obj))
  obj$con <- redux::hiredis() # ensure we can cleanup later!

  key_complete <- rrq_key_task_complete(obj$queue_id, t)
  key_status <- sprintf("%s:task:status", obj$queue_id)

  mockery::expect_called(mock_pipeline, 5)
  expect_equal(
    mockery::mock_args(mock_pipeline)[[1]],
    list(redis$BLPOP(key_complete, 1), redis$HMGET(key_status, t)))
  expect_equal(
    mockery::mock_args(mock_pipeline)[[2]],
    list(redis$BLPOP(key_complete, 1), redis$HMGET(key_status, t)))
  expect_equal(
    mockery::mock_args(mock_pipeline)[[3]],
    list(redis$BLPOP(key_complete[c(1, 3)], 1),
         redis$HMGET(key_status, t[c(1, 3)])))
  expect_equal(
    mockery::mock_args(mock_pipeline)[[4]],
    list(redis$BLPOP(key_complete[1], 1), redis$HMGET(key_status, t[1])))
  expect_equal(
    mockery::mock_args(mock_pipeline)[[5]],
    list(redis$BLPOP(key_complete[1], 1), redis$HMGET(key_status, t[1])))
})


test_that("can optionally name status elements", {
  obj <- test_rrq()
  t <- rrq_task_create_expr(sqrt(2), controller = obj)
  expect_equal(
    rrq_task_status(t, controller = obj), TASK_PENDING)
  expect_equal(
    rrq_task_status(t, named = TRUE, controller = obj),
    set_names(TASK_PENDING, t))
})


test_that("can get status of no tasks", {
  obj <- test_rrq()
  expect_equal(rrq_task_status(character(), controller = obj),
               character())
  expect_equal(rrq_task_status(character(), named = TRUE, controller = obj),
               set_names(character(), character()))
})


test_that("can test existance of no tasks", {
  obj <- test_rrq()
  expect_equal(rrq_task_exists(character(), controller = obj),
               logical())
  expect_equal(rrq_task_exists(character(), named = TRUE, controller = obj),
               set_names(logical(), character()))
})


test_that("can get times of no tasks", {
  obj <- test_rrq()
  expect_equal(rrq_task_times(character(), controller = obj),
               matrix(numeric(), 0, 4, FALSE,
                      list(NULL, c("submit", "start", "complete", "moved"))))
})


test_that("Can get results from no tasks", {
  obj <- test_rrq()
  expect_equal(rrq_task_results(character(), controller = obj),
               list())
  expect_equal(rrq_task_results(character(), named = TRUE, controller = obj),
               set_names(list(), character()))
})


test_that("overview can filter by tasks", {
  obj <- test_rrq()
  id <- rrq::rrq_task_create_bulk_call(sqrt, 1:10, controller = obj)
  empty <- rrq_task_overview(character(), controller = obj)
  expect_equal(
    empty,
    set_names(as.list(rep(0, length(TASK$all))), TASK$all))
  expect_equal(
    rrq_task_overview(controller = obj),
    modifyList(empty, list(PENDING = 10)))
  expect_equal(
    rrq_task_overview(NULL, controller = obj),
    modifyList(empty, list(PENDING = 10)))
  expect_equal(
    rrq_task_overview(id[1:3], controller = obj),
    modifyList(empty, list(PENDING = 3)))
})
