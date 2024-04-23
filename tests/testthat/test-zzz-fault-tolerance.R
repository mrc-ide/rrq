test_that("heartbeat", {
  obj <- test_rrq()

  cfg <- rrq_worker_config(heartbeat_period = 3, verbose = FALSE)
  res <- rrq_worker_config_save(WORKER_CONFIG_DEFAULT, cfg, controller = obj)

  w <- test_worker_blocking(obj)
  dat <- w$info()
  expect_equal(dat$heartbeat_key,
               rrq_key_worker_heartbeat(obj$queue_id, w$id))

  expect_equal(obj$con$EXISTS(dat$heartbeat_key), 1)
  expect_lte(obj$con$PTTL(dat$heartbeat_key),
             cfg$heartbeat_period * 3 * 1000)
  ## This might be just a bit too strict over slow connections if the
  ## worker is not close to the connection, so I've subtracted .1s off
  ## arbitrarily
  expect_gte(obj$con$PTTL(dat$heartbeat_key),
             cfg$heartbeat_period * 2 * 1000 - 100)

  w$shutdown()
  expect_equal(obj$con$EXISTS(dat$heartbeat_key), 0)
  expect_equal(rrq_worker_list(controller = obj), character(0))
})


test_that("detecting exited workers with no workers is quiet", {
  obj <- test_rrq("myfuns.R")
  expect_silent(res <- withVisible(rrq_worker_detect_exited(controller = obj)))
  expect_false(res$visible)
  expect_null(nrow(res$value))
})


test_that("detecting workers with no heartbeat is quiet", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_spawn(obj)

  expect_silent(res <- withVisible(rrq_worker_detect_exited(controller = obj)))
  expect_false(res$visible)
  expect_null(res$value)

  rrq_worker_stop(controller = obj)
  expect_silent(res <- withVisible(rrq_worker_detect_exited(controller = obj)))
  expect_false(res$visible)
  expect_null(res$value)
})


test_that("detecting output with clean exit is quiet", {
  obj <- test_rrq("myfuns.R")

  ## We need to set poll_queue to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  cfg <- rrq_worker_config(poll_queue = 1, heartbeat_period = 1,
                           verbose = FALSE)
  rrq_worker_config_save(WORKER_CONFIG_DEFAULT, cfg, controller = obj)

  w <- test_worker_spawn(obj)

  expect_silent(res <- withVisible(rrq_worker_detect_exited(controller = obj)))
  expect_false(res$visible)
  expect_null(res$value)

  w$stop()

  Sys.sleep(3)
  expect_silent(res <- withVisible(rrq_worker_detect_exited(controller = obj)))
  expect_false(res$visible)
  expect_null(res$value)

  Sys.sleep(3)

  expect_silent(res <- withVisible(rrq_worker_detect_exited(controller = obj)))
  expect_false(res$visible)
  expect_null(res$value)
})


test_that("detect killed worker (via heartbeat)", {
  skip_on_covr() # possibly causing corrupt covr output
  obj <- test_rrq("myfuns.R", timeout_worker_stop = 0)

  ## We need to set poll_queue to be fairly fast because BLPOP is not
  ## interruptable; the interrupt will only be handled _after_ R gets
  ## control back.
  cfg <- rrq_worker_config(poll_queue = 1, heartbeat_period = 1,
                           verbose = FALSE)
  res <- rrq_worker_config_save(WORKER_CONFIG_DEFAULT, cfg, controller = obj)

  w <- test_worker_spawn(obj)

  key <- rrq_key_worker_heartbeat(obj$queue_id, w$id)
  expect_equal(obj$con$EXISTS(key), 1)
  expire <- cfg$heartbeat_period * 3
  expect_equal(obj$con$GET(key), as.character(expire))
  expect_lte(obj$con$TTL(key), expire)

  t <- rrq_task_create_expr(slowdouble(10000), controller = obj)
  wait_status(t, obj, status = TASK_PENDING)
  expect_equal(rrq_task_status(t, controller = obj), TASK_RUNNING)
  expect_equal(rrq_worker_status(w$id, controller = obj),
               setNames(WORKER_BUSY, w$id))

  w$kill()
  Sys.sleep(0.1)
  expect_equal(rrq_task_status(t, controller = obj), TASK_RUNNING)
  expect_equal(rrq_worker_status(w$id, controller = obj),
               setNames(WORKER_BUSY, w$id))

  ## This is a bit annoying as it takes a while to run through;
  Sys.sleep(expire)

  ## Our key has gone!  Marvellous!
  expect_equal(obj$con$EXISTS(key), 0)

  expect_equal(rrq_worker_list(controller = obj), w$id)
  msg <- capture_messages(res <- rrq_worker_detect_exited(controller = obj))
  expect_equal(res, set_names(t, w$id))
  expect_match(msg, sprintf("Lost 1 worker:\\s+- %s", w$id),
               all = FALSE)
  expect_match(msg, sprintf("Orphaning 1 task:\\s+- %s", t),
               all = FALSE)

  expect_silent(res <- rrq_worker_detect_exited(controller = obj))
  expect_null(res)

  expect_equal(rrq_task_status(t, controller = obj), TASK_DIED)
  expect_equal(rrq_task_result(t, controller = obj),
               worker_task_failed(TASK_DIED, obj$queue_id, t))
})


## See https://github.com/mrc-ide/rrq/issues/22
test_that("detect multiple killed workers", {
  obj <- test_rrq("myfuns.R", timeout_worker_stop = 0)

  cfg <- rrq_worker_config(poll_queue = 1, heartbeat_period = 1,
                           verbose = FALSE)
  rrq_worker_config_save(WORKER_CONFIG_DEFAULT, cfg, controller = obj)

  w <- test_worker_spawn(obj, n = 2)

  t1 <- rrq_task_create_expr(slowdouble(10000), controller = obj)
  t2 <- rrq_task_create_expr(slowdouble(10000), controller = obj)
  wait_status(t1, obj, status = TASK_PENDING)
  wait_status(t2, obj, status = TASK_PENDING)

  w$kill()

  expire <- cfg$heartbeat_period * 3
  Sys.sleep(expire)

  res <- evaluate_promise(rrq_worker_detect_exited(controller = obj))

  expect_equal(length(res$result), 2)
  expect_setequal(names(res$result), w$id)
  expect_setequal(unname(res$result), c(t1, t2))

  expect_match(res$messages, "Lost 2 workers", all = FALSE)

  expect_silent(res <- rrq_worker_detect_exited(controller = obj))
  expect_null(res)

  expect_equal(rrq_task_status(t1, controller = obj), TASK_DIED)
  expect_equal(rrq_task_result(t1, controller = obj),
               worker_task_failed(TASK_DIED, obj$queue_id, t1))

  expect_equal(rrq_task_status(t2, controller = obj), TASK_DIED)
  expect_equal(rrq_task_result(t2, controller = obj),
               worker_task_failed(TASK_DIED, obj$queue_id, t2))
})


test_that("Cope with dying subprocess task", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_spawn(obj)

  path <- tempfile()
  t <- rrq_task_create_expr(pid_and_sleep(path, 600),
                            separate_process = TRUE,
                            controller = obj)

  wait_status(t, obj)
  wait_timeout("File did not appear", 10, function() !file.exists(path))

  pid_sub <- as.integer(readLines(path))
  tools::pskill(pid_sub)
  wait_status(t, obj, status = TASK_RUNNING)
  expect_equal(rrq_task_status(t, controller = obj), TASK_DIED)
  expect_equal(rrq_task_result(t, controller = obj),
               worker_task_failed(TASK_DIED, obj$queue_id, t))

  log <- rrq_worker_log_tail(w$id, Inf, controller = obj)
  expect_equal(log$command,
               c("ALIVE", "ENVIR", "ENVIR", "QUEUE",
                 "TASK_START", "REMOTE",
                 "CHILD", "ENVIR", "ENVIR", "TASK_DIED"))
})


test_that("Can wait on a retried task", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_spawn(obj)

  t1 <- rrq_task_create_expr(runif(1), controller = obj)
  rrq_task_wait(t1, controller = obj)
  r1 <- rrq_task_result(t1, controller = obj)

  t2 <- rrq_task_retry(t1, controller = obj)
  rrq_task_wait(t2, controller = obj)
  r2 <- rrq_task_result(t2, controller = obj)

  expect_type(r2, "double")
  expect_true(r1 != r2)
})


test_that("Can wait on retried tasks within bundle", {
  obj <- test_rrq()
  w <- test_worker_spawn(obj)

  ids1 <- rrq_task_create_bulk_call(
    function(i) runif(1, i, i + 1),
    1:10,
    controller = obj)
  expect_true(rrq_task_wait(ids1, timeout = 3, controller = obj))
  res1 <- rrq_task_results(ids1, controller = obj)
  expect_equal(vnapply(res1, floor), 1:10)

  ids2 <- rrq_task_retry(ids1[2:5], controller = obj)
  expect_true(rrq_task_wait(ids1, timeout = 3, controller = obj))
  res2 <- rrq_task_results(ids1, controller = obj)

  expect_equal(vnapply(res2, floor), 1:10)
  expect_equal(unlist(res2) != unlist(res1), 1:10 %in% 2:5)
  expect_equal(
    rrq_task_status(ids1, controller = obj), rep(TASK_COMPLETE, 10))
})
