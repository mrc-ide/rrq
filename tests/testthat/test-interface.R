test_that("can construct new-style object", {
  res <- test_rrq()
  expect_s3_class(res, "rrq_controller2")
  expect_setequal(names(res),
                  c("queue_id", "con", "keys",
                    "timeout_task_wait", "follow", "scripts", "store"))
})


test_that("can set default controller", {
  rrq_default_controller_clear()
  on.exit(rrq_default_controller_clear())
  res <- test_rrq()
  rrq_default_controller_set(res)
  expect_identical(pkg$default_controller, res)
  expect_identical(get_controller(NULL), res)
})


test_that("can print a controller", {
  controller <- test_rrq()
  res <- evaluate_promise(print(controller))
  expect_match(res$output, "<rrq_controller: rrq:[[:xdigit:]]{32}>")
  expect_equal(res$result, controller)
})


test_that("can get a controller or throw", {
  rrq_default_controller_clear()
  on.exit(rrq_default_controller_clear())

  r2 <- test_rrq()
  r3 <- test_rrq()

  expect_identical(get_controller(r2), r2)
  expect_error(get_controller(NULL),
               "Default controller not set")
  rrq_default_controller_set(r2)
  expect_identical(get_controller(NULL), r2)
  expect_identical(get_controller(r2), r2)
  expect_identical(get_controller(r3), r3)
})
