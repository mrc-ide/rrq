context("fault tolerance")

test_that("heartbeat", {
  Sys.setenv(R_TESTS = "")
  root <- tempfile()
  context <- context::context_save(root, sources = "myfuns.R")
  context <- context::context_load(context, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  res <- obj$worker_config_save("localhost", heartbeat_period = 3,
                                copy_redis = TRUE)
  expect_equal(res$heartbeat_period, 3)

  wid <- worker_spawn(obj, timeout = 5, progress = PROGRESS)

  dat <- obj$worker_info(wid)[[1]]
  expect_equal(dat$heartbeat_key,
               rrq_key_worker_heartbeat(context$id, wid))
  expect_equal(obj$con$EXISTS(dat$heartbeat_key), 1)
  expect_lte(obj$con$PTTL(dat$heartbeat_key),
             res$heartbeat_period * 3 * 1000)
  ## This might be just a bit too strict over slow connections if the
  ## worker is not close to the connection, so I've subtracted .1s off
  ## arbitrarily
  expect_gte(obj$con$PTTL(dat$heartbeat_key),
             res$heartbeat_period * 2 * 1000 - 100)

  res <- obj$worker_stop(wid, timeout = 1)
  expect_equal(obj$con$EXISTS(dat$heartbeat_key), 0)
  expect_equal(obj$worker_list(), character(0))
})
