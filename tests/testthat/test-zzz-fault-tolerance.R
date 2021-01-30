context("fault tolerance")

test_that("heartbeat", {
  obj <- test_rrq()

  res <- obj$worker_config_save("localhost", heartbeat_period = 3)
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

test_that("interrupt stuck worker (local)", {
  ## This one tests that if a worker is stuck on a long running task
  ## that we can shunt them off it.  It will not work on windows
  ## because there is no concept of interrupt that we can easily use.
  skip_on_os("windows")
  ## This fails on covr with the worker disappearing
  skip_on_covr()
  ## Fails on CI too, at least for ubuntu/R-devel; mrc-2094
  skip_on_ci()

  obj <- test_rrq("myfuns.R")

  ## We need to set time_poll to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  res <- obj$worker_config_save("localhost", time_poll = 1)

  wid <- test_worker_spawn(obj)
  pid <- obj$worker_info()[[wid]]$pid

  expect_equal(obj$message_send_and_wait("PING", timeout = 10),
               setNames(list("PONG"), wid))

  t <- obj$enqueue(slowdouble(10000))
  wait_status(t, obj, status = TASK_PENDING)
  expect_equal(obj$task_status(t), setNames(TASK_RUNNING, t))
  expect_equal(obj$worker_status(wid), setNames(WORKER_BUSY, wid))

  tools::pskill(pid, tools::SIGINT)
  wait_status(t, obj, status = TASK_RUNNING)

  expect_equal(obj$task_status(t), setNames(TASK_INTERRUPTED, t))
  expect_equal(obj$worker_status(wid), setNames(WORKER_IDLE, wid))

  expect_equal(obj$message_send_and_wait("PING", timeout = 10),
               setNames(list("PONG"), wid))

  ## Then try the interrupt _during_ a string of messages and be sure
  ## that the messages get requeued correctly.
  tools::pskill(pid, tools::SIGINT)
  expect_equal(obj$message_send_and_wait("PING", timeout = 10),
               setNames(list("PONG"), wid))

  tmp <- obj$worker_log_tail(wid, 3L)
  expect_equal(tmp$command,
               c("REQUEUE", "MESSAGE", "RESPONSE"))
})

test_that("interrupt stuck worker (via heartbeat)", {
  ## Basically the same test as above, but we'll do it via the
  ## heartbeat thread.  These might be worth merging.
  skip_on_os("windows")
  ## This fails on covr with the worker disappearing
  skip_on_covr()

  obj <- test_rrq("myfuns.R")

  ## We need to set time_poll to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  res <- obj$worker_config_save("localhost", time_poll = 1,
                                heartbeat_period = 3)

  wid <- test_worker_spawn(obj)

  expect_equal(obj$message_send_and_wait("PING", timeout = 10),
               setNames(list("PONG"), wid))

  t <- obj$enqueue(slowdouble(10000))
  wait_status(t, obj, status = TASK_PENDING)
  expect_equal(obj$task_status(t), setNames(TASK_RUNNING, t))
  expect_equal(obj$worker_status(wid), setNames(WORKER_BUSY, wid))

  worker_send_signal(obj$con, obj$keys, tools::SIGINT, wid)
  wait_status(t, obj, status = TASK_RUNNING)

  expect_equal(obj$task_status(t), setNames(TASK_INTERRUPTED, t))
  expect_equal(obj$worker_status(wid), setNames(WORKER_IDLE, wid))

  expect_equal(obj$message_send_and_wait("PING", timeout = 10),
               setNames(list("PONG"), wid))

  ## Then try the interrupt _during_ a string of messages and be sure
  ## that the messages get requeued correctly.
  worker_send_signal(obj$con, obj$keys, tools::SIGINT, wid)
  expect_equal(obj$message_send_and_wait("PING", timeout = 10),
               setNames(list("PONG"), wid))

  tmp <- obj$worker_log_tail(wid, 3L)
  expect_equal(tmp$command,
               c("REQUEUE", "MESSAGE", "RESPONSE"))
})


test_that("Stop task with interrupt", {
  skip_on_os("windows")
  obj <- test_rrq("myfuns.R")

  ## We need to set time_poll to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  res <- obj$worker_config_save("localhost", time_poll = 1,
                                heartbeat_period = 1)

  wid <- test_worker_spawn(obj)
  pid <- obj$worker_info()[[wid]]$pid

  t <- obj$enqueue(slowdouble(10000))
  wait_status(t, obj, status = TASK_PENDING)
  expect_equal(obj$task_status(t), setNames(TASK_RUNNING, t))
  expect_equal(obj$worker_status(wid), setNames(WORKER_BUSY, wid))

  res <- obj$task_cancel(t)
  expect_true(res)
  wait_status(t, obj, status = TASK_RUNNING)
  wait_worker_status(wid, obj, status = WORKER_BUSY)
  wait_worker_status(wid, obj, status = WORKER_PAUSED)

  expect_equal(obj$task_status(t), setNames(TASK_INTERRUPTED, t))
  expect_equal(obj$worker_status(wid), setNames(WORKER_IDLE, wid))

  ## These checks don't work for covr for unknown reasons, probably to
  ## do with signal handling.
  skip_on_covr()
  log <- obj$worker_log_tail(wid, Inf)
  expect_equal(log$command, c("ALIVE",
                              "TASK_START", "INTERRUPT", "TASK_INTERRUPTED",
                              "MESSAGE", "RESPONSE", "MESSAGE", "RESPONSE"))
  expect_equal(log$message, c("", t, "", t,
                              "PAUSE", "PAUSE", "RESUME", "RESUME"))
})


test_that("race conditioning handling when cancelling tasks", {
  skip_on_os("windows")

  obj <- test_rrq("myfuns.R")

  ## We need to set time_poll to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  res <- obj$worker_config_save("localhost", time_poll = 1,
                                heartbeat_period = 1)
  w <- test_worker_blocking(obj)
  w$heartbeat$stop() # don't *actually* do the interrupt!

  t <- obj$enqueue(slowdouble(10))
  w$poll(TRUE)
  worker_run_task_start(w, t)

  info <- w$info()
  info$heartbeat_key <- NULL
  w$con$HSET(w$keys$worker_info, w$name, object_to_bin(info))

  ## We can do this test too while we're here.
  expect_error(obj$task_cancel(t), "Worker does not have heartbeat enabled")

  obj$con$HSET(obj$keys$worker_status, w$name, WORKER_IDLE)
  expect_error(obj$task_cancel(t), "Task finished during check")
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
  obj <- test_rrq("myfuns.R")

  ## We need to set time_poll to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  res <- obj$worker_config_save("localhost", time_poll = 1,
                                heartbeat_period = 1)

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
  obj <- test_rrq("myfuns.R")

  ## We need to set time_poll to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  res <- obj$worker_config_save("localhost", time_poll = 1,
                                heartbeat_period = 1)

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
})


## See https://github.com/mrc-ide/rrq/issues/22
test_that("detect multiple killed workers", {
  obj <- test_rrq("myfuns.R")

  res <- obj$worker_config_save("localhost", time_poll = 1,
                                heartbeat_period = 1)

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

  res <- obj$worker_detect_exited()

  expect_equal(length(res), 2)
  expect_setequal(names(res), wid)
  expect_setequal(unname(res), c(t1, t2))

  expect_silent(res <- obj$worker_detect_exited())
  expect_null(res)
})
