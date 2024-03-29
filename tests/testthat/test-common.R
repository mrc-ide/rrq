test_that("task all contains all task statuses", {
  all_task_keys <- ls(pattern = "^TASK_", envir = asNamespace("rrq"))
  ## Deliberately exclude TASK_MISSING as this is not a status
  ## a real task can have, it is the NULL status given to
  ## non-existent tasks
  all_task_keys <- setdiff(all_task_keys, "TASK_MISSING")
  task_all <- vcapply(all_task_keys,
                      function(key) get(key, envir = asNamespace("rrq")))
  expect_setequal(task_all, TASK$all)
})

test_that("can get version info", {
  expect_match(version_info(), "\\d+\\.\\d+\\.\\d+")
})
