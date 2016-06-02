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
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())

  ## This needs to send output to a file and not to stdout!
  wid <- worker_spawn(obj$context, obj$con, logfile)
  on.exit(test_queue_clean(obj$context$id))

  t <- obj$enqueue(slowdouble(0.1))
  expect_is(t, "character")
  expect_equal(obj$task_wait(t, 2), 0.2)
  expect_equal(obj$task_result(t), 0.2)
})

test_that("bulk", {
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  env <- context::context_load(context, FALSE)
  obj <- rrq_controller(context, redux::hiredis(), env)

  n_workers <- 5
  x <- runif(n_workers * 2)

  wid <- worker_spawn(obj$context, obj$con, n_workers, "logs")
  on.exit(test_queue_clean(obj$context$id))
  expect_true(all(file.exists(file.path("logs", paste0(wid, ".log")))))

  res <- obj$lapply(x, quote(slowdouble), progress_bar=FALSE)

  expect_equal(res, as.list(x * 2))

  test_queue_clean(obj$context$id)
  on.exit()

  expect_equal(redux::scan_find(obj$con, sprintf("rrq:%s*", context$id)),
               character(0))
})

test_that("worker name", {
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())

  name <- ids::random_id()
  wid <- worker_spawn(obj$context, obj$con,
                      logdir="logs", worker_name_base=name)
  on.exit(test_queue_clean(obj$context$id))
  expect_equal(wid, paste(name, "_1"))
  expect_true(file.exists(file.path("logs", paste0(name, "_1.log"))))
})
