test_that("heartbeat", {
  skip_if_not_installed("callr")
  obj <- test_rrq()

  res <- obj$worker_config_save("localhost", heartbeat_period = 3,
                                verbose = FALSE)
  expect_equal(res$heartbeat_period, 3)

  w <- test_worker_blocking(obj)
  dat <- w$info()
  expect_equal(dat$heartbeat_key,
               rrq_key_worker_heartbeat(obj$queue_id, w$name))

  expect_equal(obj$con$EXISTS(dat$heartbeat_key), 1)
  expect_lte(obj$con$PTTL(dat$heartbeat_key),
             res$heartbeat_period * 3 * 1000)
  ## This might be just a bit too strict over slow connections if the
  ## worker is not close to the connection, so I've subtracted .1s off
  ## arbitrarily
  expect_gte(obj$con$PTTL(dat$heartbeat_key),
             res$heartbeat_period * 2 * 1000 - 100)

  w$shutdown()
  expect_equal(obj$con$EXISTS(dat$heartbeat_key), 0)
  expect_equal(obj$worker_list(), character(0))
})


test_that("detecting exited workers with no workers is quiet", {
  obj <- test_rrq("myfuns.R")
  expect_silent(res <- withVisible(obj$worker_detect_exited()))
  expect_false(res$visible)
  expect_null(nrow(res$value))
})


test_that("detecting workers with no heartbeat is quiet", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj)

  expect_silent(res <- withVisible(obj$worker_detect_exited()))
  expect_false(res$visible)
  expect_null(res$value)

  obj$worker_stop()
  expect_silent(res <- withVisible(obj$worker_detect_exited()))
  expect_false(res$visible)
  expect_null(res$value)
})


test_that("detecting output with clean exit is quiet", {
  skip_if_not_installed("callr")
  obj <- test_rrq("myfuns.R")

  ## We need to set time_poll to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  res <- obj$worker_config_save("localhost", time_poll = 1,
                                heartbeat_period = 1,
                                verbose = FALSE)

  wid <- test_worker_spawn(obj)
  pid <- obj$worker_info()[[wid]]$pid

  expect_silent(res <- withVisible(obj$worker_detect_exited()))
  expect_false(res$visible)
  expect_null(res$value)

  obj$worker_stop(wid)

  Sys.sleep(3)
  expect_silent(res <- withVisible(obj$worker_detect_exited()))
  expect_false(res$visible)
  expect_null(res$value)

  Sys.sleep(3)

  expect_silent(res <- withVisible(obj$worker_detect_exited()))
  expect_false(res$visible)
  expect_null(res$value)
})


test_that("detect killed worker (via heartbeat)", {
  skip_if_not_installed("callr")
  skip_on_covr() # possibly causing corrupt covr output
  obj <- test_rrq("myfuns.R", timeout_worker_stop = 0)

  ## We need to set time_poll to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  res <- obj$worker_config_save("localhost", time_poll = 1,
                                heartbeat_period = 1,
                                verbose = FALSE)

  wid <- test_worker_spawn(obj)
  pid <- obj$worker_info()[[wid]]$pid

  key <- rrq_key_worker_heartbeat(obj$queue_id, wid)
  expect_equal(obj$con$EXISTS(key), 1)
  expire <- res$heartbeat_period * 3
  expect_equal(obj$con$GET(key), as.character(expire))
  expect_lte(obj$con$TTL(key), expire)

  t <- obj$enqueue(slowdouble(10000))
  wait_status(t, obj, status = TASK_PENDING)
  expect_equal(obj$task_status(t), setNames(TASK_RUNNING, t))
  expect_equal(obj$worker_status(wid), setNames(WORKER_BUSY, wid))

  tools::pskill(pid, tools::SIGTERM)
  Sys.sleep(0.1)
  expect_equal(obj$task_status(t), setNames(TASK_RUNNING, t))
  expect_equal(obj$worker_status(wid), setNames(WORKER_BUSY, wid))

  ## This is a bit annoying as it takes a while to run through;
  Sys.sleep(expire)

  ## Our key has gone!  Marvellous!
  expect_equal(obj$con$EXISTS(key), 0)

  expect_equal(obj$worker_list(), wid)
  msg <- capture_messages(res <- obj$worker_detect_exited())
  expect_equal(res, set_names(t, wid))
  expect_match(msg, sprintf("Lost 1 worker:\\s+- %s", wid),
               all = FALSE)
  expect_match(msg, sprintf("Orphaning 1 task:\\s+- %s", t),
               all = FALSE)

  expect_silent(res <- obj$worker_detect_exited())
  expect_null(res)

  expect_equal(obj$task_status(t), set_names(TASK_DIED, t))
  expect_equal(obj$task_result(t),
               worker_task_failed(TASK_DIED, obj$queue_id, t))
})


## See https://github.com/mrc-ide/rrq/issues/22
test_that("detect multiple killed workers", {
  skip_if_not_installed("callr")
  obj <- test_rrq("myfuns.R", timeout_worker_stop = 0)

  res <- obj$worker_config_save("localhost", time_poll = 1,
                                heartbeat_period = 1, verbose = FALSE)

  wid <- test_worker_spawn(obj, n = 2)
  pid <- vnapply(obj$worker_info()[wid], "[[", "pid")

  t1 <- obj$enqueue(slowdouble(10000))
  t2 <- obj$enqueue(slowdouble(10000))
  wait_status(t1, obj, status = TASK_PENDING)
  wait_status(t2, obj, status = TASK_PENDING)

  tools::pskill(pid[[1]], tools::SIGTERM)
  tools::pskill(pid[[2]], tools::SIGTERM)

  expire <- res$heartbeat_period * 3
  Sys.sleep(expire)

  res <- evaluate_promise(obj$worker_detect_exited())

  expect_equal(length(res$result), 2)
  expect_setequal(names(res$result), wid)
  expect_setequal(unname(res$result), c(t1, t2))

  expect_match(res$messages, "Lost 2 workers", all = FALSE)

  expect_silent(res <- obj$worker_detect_exited())
  expect_null(res)

  expect_equal(obj$task_status(t1), set_names(TASK_DIED, t1))
  expect_equal(obj$task_result(t1),
               worker_task_failed(TASK_DIED, obj$queue_id, t1))

  expect_equal(obj$task_status(t2), set_names(TASK_DIED, t2))
  expect_equal(obj$task_result(t2),
               worker_task_failed(TASK_DIED, obj$queue_id, t2))
})


test_that("Cope with dying subprocess task", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj)

  path <- tempfile()
  t <- obj$enqueue(pid_and_sleep(path, 600), separate_process = TRUE)

  wait_status(t, obj)
  wait_timeout("File did not appear", 10, function() !file.exists(path))

  pid_sub <- as.integer(readLines(path))
  tools::pskill(pid_sub)
  wait_status(t, obj, status = TASK_RUNNING)
  expect_equal(obj$task_status(t), set_names(TASK_DIED, t))
  expect_equal(obj$task_result(t),
               worker_task_failed(TASK_DIED, obj$queue_id, t))

  log <- obj$worker_log_tail(wid, Inf)
  expect_equal(log$command,
               c("ALIVE", "TASK_START", "REMOTE", "TASK_DIED"))
})


test_that("Can wait on a retried task", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj)

  t1 <- obj$enqueue(runif(1))
  r1 <- obj$task_wait(t1)

  t2 <- obj$task_retry(t1)
  r2 <- obj$task_wait(t2)

  expect_type(r2, "double")
  expect_true(r1 != r2)
})


test_that("Can wait on retried tasks within bundle", {
  obj <- test_rrq()
  wid <- test_worker_spawn(obj)

  grp <- obj$lapply(1:10, function(i) runif(1, i, i + 1), timeout_task_wait = 0)
  res1 <- obj$bulk_wait(grp, delete = FALSE, timeout = 3)
  expect_equal(vnapply(res1, floor), 1:10)

  ## So, hitting this immediately does not work:
  ids <- obj$task_retry(grp$task_ids[2:5])
  res2 <- obj$bulk_wait(grp, time_poll = 1, timeout = 3, delete = FALSE)

  expect_equal(vnapply(res2, floor), 1:10)
  expect_equal(unlist(res2) != unlist(res1), 1:10 %in% 2:5)
  expect_equal(unname(obj$task_status(ids)), rep(TASK_COMPLETE, 4))
})
