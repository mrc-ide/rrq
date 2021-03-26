context("bulk")

test_that("lapply simple case", {
  obj <- test_rrq()
  obj$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                         time_poll = 1, overwrite = TRUE)
  w <- test_worker_blocking(obj)

  grp <- obj$lapply_(1:10, quote(log), dots = list(base = 2), timeout = 0)
  expect_is(grp, "rrq_bulk")
  expect_setequal(names(grp), c("task_ids", "key_complete", "names"))

  w$loop(immediate = TRUE)

  res <- obj$bulk_wait(grp)
  expect_equal(res, as.list(log2(1:10)))

  ## tasks are deleted on collection
  expect_equal(obj$task_status(grp$task_ids),
               set_names(rep(TASK_MISSING, 10), grp$task_ids))

  log <- obj$worker_log_tail(w$name, Inf)
  expect_equal(log$command,
               c("ALIVE",
                 rep(c("TASK_START", "TASK_COMPLETE"), 10),
                 "STOP"))
})


test_that("lapply with anonymous function", {
  obj <- test_rrq()
  obj$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                         time_poll = 1, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  grp <- obj$lapply_(1:10, function(x) x + 1, timeout = 0)
  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(res, as.list(2:11))
})


test_that("NSE - use namespaced function", {
  obj <- test_rrq()
  obj$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                         time_poll = 1, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  grp <- obj$lapply(1:10, ids::adjective_animal, timeout = 0)

  dat <- bin_to_object(obj$con$HGET(obj$keys$task_expr, grp$task_ids[[1]]))
  expect_equal(dat, list(expr = quote(ids::adjective_animal(1L))))

  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(lengths(res), 1:10)
  expect_match(unlist(res), "^[a-z]+_[a-z]+$")
})


test_that("NSE - use namespaced function with lazy dots", {
  obj <- test_rrq()
  obj$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                         time_poll = 1, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  mystyle <- "camel"
  grp <- obj$lapply(1:10, ids::adjective_animal, style = mystyle, timeout = 0)

  dat <- bin_to_object(obj$con$HGET(obj$keys$task_expr, grp$task_ids[[1]]))
  expect_equal(dat,
               list(expr = quote(ids::adjective_animal(1L, style = mystyle)),
                    objects = c(mystyle = obj$db$hash_object(mystyle))))

  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(lengths(res), 1:10)
  expect_match(unlist(res), "^[a-z]+[A-Z][a-z]+$")
})


test_that("lapply blocking", {
  obj <- test_rrq()
  w <- test_worker_spawn(obj)
  res <- obj$lapply(1:10, sqrt, timeout = 1)
  expect_equal(res, as.list(sqrt(1:10)), timeout = 10)
})


test_that("lapply to alt queue", {
  obj <- test_rrq()
  obj$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                         time_poll = 1, overwrite = TRUE, queue = "a")
  w <- test_worker_blocking(obj)

  grp <- obj$lapply_(1:10, quote(log), dots = list(base = 2), timeout = 0,
                     queue = "a")
  expect_equal(obj$queue_length("a"), 10)
  expect_equal(obj$queue_length(), 0)

  w$loop(immediate = TRUE)

  expect_equal(obj$bulk_wait(grp), as.list(log(1:10, base = 2)))
})


test_that("bulk tasks can be queued with dependency", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(sin(0))
  t2 <- obj$enqueue(sin(pi / 2))
  grp <- obj$lapply(c(0, pi / 2), sin, timeout = 0, depends_on = c(t, t2))
  expect_equal(obj$queue_list(), c(t, t2))
  t3 <- obj$enqueue(sin(pi / 2), depends_on = grp$task_ids)
  ## t3 has not been added to main queue yet
  expect_equal(obj$queue_list(), c(t, t2))
  expect_equivalent(obj$task_status(t), "PENDING")
  expect_equivalent(obj$task_status(t2), "PENDING")
  expect_equivalent(obj$task_status(grp$task_ids), c("DEFERRED", "DEFERRED"))
  expect_equivalent(obj$task_status(t3), "DEFERRED")

  ## Original dependencies are stored
  grp_id_1 <- grp$task_ids[[1]]
  grp_id_2 <- grp$task_ids[[2]]
  original_grp_dep_id_1 <- rrq_key_task_dependencies_original(
    obj$keys$queue_id, grp_id_1)
  expect_setequal(obj$con$SMEMBERS(original_grp_dep_id_1), c(t, t2))
  original_grp_dep_id_2 <- rrq_key_task_dependencies_original(
    obj$keys$queue_id, grp_id_2)
  expect_setequal(obj$con$SMEMBERS(original_grp_dep_id_2), c(t, t2))
  original_deps_keys_t3 <- rrq_key_task_dependencies_original(
    obj$keys$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(original_deps_keys_t3), grp$task_ids)

  ## Pending dependencies are stored
  grp_dep_id_1 <- rrq_key_task_dependencies(obj$keys$queue_id, grp_id_1)
  expect_setequal(obj$con$SMEMBERS(grp_dep_id_1), c(t, t2))
  grp_dep_id_2 <- rrq_key_task_dependencies(obj$keys$queue_id, grp_id_2)
  expect_setequal(obj$con$SMEMBERS(grp_dep_id_2), c(t, t2))
  dependency_keys_t3 <- rrq_key_task_dependencies(obj$keys$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(dependency_keys_t3), grp$task_ids)

  ## Inverse depends_on relationship is stored
  dependent_keys_t <- rrq_key_task_dependents(obj$keys$queue_id, t)
  for (key in dependent_keys_t) {
    expect_setequal(obj$con$SMEMBERS(key), grp$task_ids)
  }
  dependent_keys_t2 <- rrq_key_task_dependents(obj$keys$queue_id, t2)
  for (key in dependent_keys_t2) {
    expect_setequal(obj$con$SMEMBERS(key), grp$task_ids)
  }
  grp_dependents_1 <- rrq_key_task_dependents(obj$keys$queue_id, grp_id_1)
  for (key in grp_dependents_1) {
    expect_setequal(obj$con$SMEMBERS(key), t3)
  }
  grp_dependents_2 <- rrq_key_task_dependents(obj$keys$queue_id, grp_id_2)
  for (key in grp_dependents_2) {
    expect_setequal(obj$con$SMEMBERS(key), t3)
  }

  ## Items are in deferred queue
  deferred_set <- obj$keys$deferred_set
  expect_setequal(obj$con$SMEMBERS(deferred_set), c(grp$task_ids, t3))

  w$step(TRUE)
  obj$task_wait(t, 2)
  expect_equivalent(obj$task_status(t), "COMPLETE")
  expect_equivalent(obj$task_status(t2), "PENDING")
  expect_equivalent(obj$task_status(grp$task_ids), c("DEFERRED", "DEFERRED"))
  expect_equivalent(obj$task_status(t3), "DEFERRED")

  w$step(TRUE)
  obj$task_wait(t2, 2)
  expect_equivalent(obj$task_status(t2), "COMPLETE")
  expect_equivalent(obj$task_status(grp$task_ids), c("PENDING", "PENDING"))
  expect_equivalent(obj$task_status(t3), "DEFERRED")
  queue <- obj$queue_list()
  expect_setequal(queue, grp$task_ids)
  expect_equal(obj$con$SMEMBERS(deferred_set), list(t3))

  w$step(TRUE)
  obj$task_wait(queue[1], 2)
  expect_setequal(obj$task_status(grp$task_ids), c("COMPLETE", "PENDING"))
  expect_equivalent(obj$task_status(t3), "DEFERRED")
  expect_equal(obj$queue_list(), queue[2])
  expect_equal(obj$con$SMEMBERS(deferred_set), list(t3))

  w$step(TRUE)
  obj$task_wait(queue[2], 2)
  expect_equivalent(obj$task_status(grp$task_ids), c("COMPLETE", "COMPLETE"))
  expect_equivalent(obj$task_status(t3), "PENDING")
  expect_equal(obj$queue_list(), t3)
  expect_equal(obj$con$SMEMBERS(deferred_set), list())

  w$step(TRUE)
  obj$task_wait(t3, 2)
  expect_equivalent(obj$task_status(t3), "COMPLETE")
  expect_equal(obj$queue_list(), character(0))
})
