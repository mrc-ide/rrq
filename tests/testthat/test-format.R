test_that("Can print simple errors", {
  ## These are the ones we get that result not from the original
  ## failure, but when something nasty has happened.
  e <- worker_task_failed(TASK_DIED, "abc", "123")
  s <- format(e)
  expect_equal(
    s,
    c("<rrq_task_error>",
      "  error:  Task not successful: DIED",
      "  queue:  abc", "  task:   123",
      "  status: DIED",
      "  * To throw this error, use stop() with it"))

  res <- testthat::evaluate_promise(withVisible(print(e)))
  expect_equal(res$result, list(value = e, visible = FALSE))
  expect_equal(res$output, paste(s, collapse = "\n"))
})


test_that("Can print complex errors", {
  f1 <- function(x) f2(x)
  f2 <- function(x) {
    warning("warning 1")
    f3(x)
  }
  f3 <- function(x) {
    warning("warning 2")
    f4(x)
  }
  f4 <- function(x) {
    stop("some deep error")
  }

  suppressWarnings(
    e <- rrq_task_error(expression_eval_safely(f1(1), new.env())$value,
                        TASK_ERROR, "abc", "123"))
  s <- format(e)
  expect_equal(
    s,
    c("<rrq_task_error>",
      "  from:   f4(x)",
      "  error:  some deep error",
      "  queue:  abc",
      "  task:   123",
      "  status: ERROR",
      "  * To throw this error, use stop() with it",
      "  * This error has a stack trace, use '$trace' to see it",
      "  * This error has warnings, use '$warnings' to see them"))

  res <- testthat::evaluate_promise(withVisible(print(e)))
  expect_equal(res$result, list(value = e, visible = FALSE))
  expect_equal(res$output, paste(s, collapse = "\n"))
})

test_that("can print worker info", {
  skip_if_not_installed("callr")
  skip_on_os("windows")

  obj <- test_rrq()
  res <- obj$worker_config_save("localhost", heartbeat_period = 3)
  wid <- test_worker_spawn(obj)
  wid2 <- test_worker_spawn(obj)
  on.exit(obj$worker_stop(wid, "kill_local"))
  on.exit(obj$worker_stop(wid2, "kill_local"), add = TRUE)

  info <- obj$worker_info()
  text <- testthat::evaluate_promise(withVisible(print(info)))
  keys_regex <- c(
    "  rrq_version:\\s*[\\d\\.]+",
    "  platform: .+",
    "  running: .+",
    "  hostname: .+",
    "  username: .+",
    "  queue: .+",
    "  wd: .+",
    "  pid:\\s*\\d+",
    "  redis_host:\\s*[\\d\\.]+",
    "  redis_port:\\s*6379",
    "  heartbeat_key:\\s*rrq:.+"
  )
  expect_match(
    text$output,
    paste0(c(
      "<rrq_worker_info>",
      sprintf("  name:\\s*%s", wid),
      keys_regex,
      "<rrq_worker_info>",
      sprintf("  name:\\s*%s", wid2),
      keys_regex),
    collapse = "\\n"), perl = TRUE)
})
