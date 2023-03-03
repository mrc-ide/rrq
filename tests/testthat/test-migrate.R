test_that("Can migrate storage", {
  skip_if_no_redis()
  dat <- readRDS("migrate/0.3.1.rds")
  con <- test_hiredis()

  ## Ensure that the db does not exist, then refresh
  redux::scan_del(con, paste0(dat$queue_id, ":*"))

  invisible(lapply(dat$data, redis_restore, con))

  expect_error(
    rrq_controller$new(dat$queue_id),
    "rrq database needs migrating; please run")
  expect_error(
    rrq_worker_from_config(dat$queue_id, "localhost"),
    "rrq database needs migrating; please run")

  res <- evaluate_promise(
    rrq_migrate(dat$queue_id))
  expect_match(res$messages, "Migrating rrq database", all = FALSE)
  expect_match(res$messages, "Updated 4 / 5 task expressions", all = FALSE)
  expect_match(res$messages, "Updated 5 task results", all = FALSE)

  expect_message(
    rrq_migrate(dat$queue_id),
    "rrq database is up-to-date")

  obj <- rrq_controller$new(dat$queue_id)
  expect_setequal(obj$task_list(), names(dat$tasks))

  expect_equal(obj$tasks_result(names(dat$tasks)), dat$tasks)

  ## Rerun the tasks to show we can load them correctly
  con$HMSET(
    queue_keys(obj)$task_status,
    names(dat$tasks),
    rep(TASK_PENDING, length(dat$tasks)))
  con$RPUSH(rrq_key_queue(obj$queue_id, NULL), names(dat$tasks))

  w <- test_worker_blocking(obj)
  w$loop(TRUE)

  expect_equal(obj$tasks_result(names(dat$tasks)), dat$tasks)
})
