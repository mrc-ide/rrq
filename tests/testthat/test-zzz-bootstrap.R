context("bootstrap")

test_that("bootstrap", {
  skip("not working")
  skip_if_not_installed("provisionr")
  skip_if_no_internet()
  path <- tempfile()

  ctx <- context::context_save(path, packages = list(loaded = "rrq"))
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  context::provision_context(ctx, quiet = TRUE)

  lib <- context:::path_library(path)
  expect_true(all(c("context", "rrq", "queuer", "redux") %in% dir(lib)))

  obj <- rrq_controller(ctx, redux::hiredis())
  on.exit(obj$destroy())

  Sys.setenv(CONTEXT_BOOTSTRAP = "TRUE")
  on.exit(Sys.unsetenv("CONTEXT_BOOTSTRAP"), add = TRUE)
  wid <- worker_spawn(obj, timeout = 5, progress = FALSE)

  ## Worker reports lib on startup:
  log <- obj$worker_process_log(wid)
  expect_equal(trimws(log$value[[which(log$title == "lib")]]),
               normalizePath(lib))

  t <- obj$enqueue(.libPaths())
  res <- obj$task_wait(t, 10, progress = FALSE)

  expect_equal(normalizePath(res[[1]]), normalizePath(lib))

  t <- obj$enqueue(find.package("rrq"))
  res <- obj$task_wait(t, 10, progress = FALSE)
  expect_equal(res, file.path(lib, "rrq"))
})
