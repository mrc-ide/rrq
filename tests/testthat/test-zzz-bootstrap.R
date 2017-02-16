context("bootstrap")

test_that("bootstrap", {
  skip_if_not_installed("provisionr")
  path <- tempfile()

  ctx <- context::context_save(path, packages = list(loaded = "rrq"))
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  context::provision_context(ctx, quiet = TRUE)

  lib <- context:::path_library(path)
  expect_true(all(c("context", "rrq", "queuer", "redux") %in% dir(lib)))

  obj <- rrq_controller(ctx, redux::hiredis())
  on.exit(obj$destroy())

  wid <- workers_spawn(obj, timeout = 5, progress = FALSE)
  obj$worker_process_log(wid)

  t <- obj$enqueue(.libPaths())
  res <- obj$task_wait(t, 10, progress = FALSE)

  expect_equal(normalizePath(res[[1]]), normalizePath(lib))

  t <- obj$enqueue(find.package("rrq"))
  res <- obj$task_wait(t, 10, progress = FALSE)
  expect_equal(res, file.path(lib, "rrq"))
})
