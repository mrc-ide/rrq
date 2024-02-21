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
  skip_on_os("windows")

  obj <- test_rrq()
  cfg <- rrq_worker_config(heartbeat_period = 3)
  res <- obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg)
  w1 <- test_worker_spawn(obj)
  w2 <- test_worker_spawn(obj)

  info <- obj$worker_info()
  text <- testthat::evaluate_promise(withVisible(print(info)))
  text <- strsplit(text$output, "\n")[[1]]
  expect_equal(sum(text == "  <rrq_worker_info>"), 2)
  expect_true(any(text == paste0("$", w1$id)))
  expect_true(any(text == paste0("$", w2$id)))
  expect_true(any(grepl(paste0("    id: \\s*", w1$id), text)))
  expect_true(any(grepl(paste0("    id: \\s*", w2$id), text)))
  for (name in names(info[[1]])) {
    expect_true(any(grepl(name, text)))
  }
})
