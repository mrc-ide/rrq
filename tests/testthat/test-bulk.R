test_that("lapply simple case", {
  obj <- test_rrq()
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)
  w <- test_worker_blocking(obj)

  grp <- obj$lapply_(1:10, quote(log), dots = list(base = 2),
                     timeout_task_wait = 0)
  expect_s3_class(grp, "rrq_bulk")
  expect_setequal(names(grp), c("task_ids", "key_complete", "names"))

  w$loop(immediate = TRUE)

  res <- obj$bulk_wait(grp, delete = TRUE)
  expect_equal(res, as.list(log2(1:10)))

  ## tasks are deleted on collection
  expect_equal(obj$task_status(grp$task_ids),
               set_names(rep(TASK_MISSING, 10), grp$task_ids))

  log <- obj$worker_log_tail(w$id, Inf)
  expect_equal(log$command,
               c("ALIVE", "ENVIR", "ENVIR", "QUEUE",
                 rep(c("TASK_START", "TASK_COMPLETE"), 10),
                 "STOP"))
})


test_that("lapply with anonymous function", {
  obj <- test_rrq()
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  grp <- obj$lapply_(1:10, function(x) x + 1, timeout_task_wait = 0)
  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(res, as.list(2:11))
})


test_that("bulk add with ordinary function", {
  p <- data.frame(a = runif(10), b = runif(10))
  p$b[3] <- NA
  obj <- test_rrq()
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  grp <- obj$enqueue_bulk(p, sum, na.rm = TRUE, timeout_task_wait = 0)
  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(res, as.list(rowSums(p, na.rm = TRUE)))
})


test_that("bulk add with anonymous functions", {
  p <- data.frame(a = runif(10), b = runif(10))
  p[2] <- NA
  obj <- test_rrq()
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  grp <- obj$enqueue_bulk_(p, function(a, b) a + b, timeout_task_wait = 0)
  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(res, as.list(p$a + p$b))
})


test_that("NSE - use namespaced function", {
  obj <- test_rrq()
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  grp <- obj$lapply(1:10, ids::adjective_animal, timeout_task_wait = 0)

  dat <- bin_to_object(
    obj$con$HGET(queue_keys(obj)$task_expr, grp$task_ids[[1]]))
  expect_equal(dat, list(expr = quote(ids::adjective_animal(1L))))

  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(lengths(res), 1:10)
  expect_match(unlist(res), "^[a-z]+_[a-z]+$")
})


test_that("NSE - use namespaced function with lazy dots", {
  obj <- test_rrq()
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  mystyle <- "camel"
  grp <- obj$lapply(1:10, ids::adjective_animal, style = mystyle,
                    timeout_task_wait = 0)

  dat <- bin_to_object(
    obj$con$HGET(queue_keys(obj)$task_expr, grp$task_ids[[1]]))
  expect_equal(dat,
               list(expr = quote(ids::adjective_animal(1L, style = mystyle)),
                    objects = c(mystyle = hash_data(object_to_bin(mystyle)))))

  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(lengths(res), 1:10)
  expect_match(unlist(res), "^[a-z]+[A-Z][a-z]+$")
})


test_that("lapply blocking", {
  obj <- test_rrq()
  w <- test_worker_spawn(obj)
  res <- obj$lapply(1:10, sqrt, timeout_task_wait = 1)
  expect_equal(res, as.list(sqrt(1:10)))
})


test_that("enqueue bulk blocking", {
  obj <- test_rrq()
  p <- data.frame(a = runif(5), b = runif(5))
  f <- function(a, b) a + b
  w <- test_worker_spawn(obj)
  res <- obj$enqueue_bulk(p, f, timeout_task_wait = 1)
  expect_equal(res, as.list(rowSums(p)))
})


test_that("lapply to alt queue", {
  obj <- test_rrq()
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1,
                           queue = "a")
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)
  w <- test_worker_blocking(obj)

  grp <- obj$lapply_(1:10, quote(log), dots = list(base = 2),
                     timeout_task_wait = 0, queue = "a")
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
  grp <- obj$lapply(c(0, pi / 2), sin, timeout_task_wait = 0,
                    depends_on = c(t, t2))
  expect_equal(obj$queue_list(), c(t, t2))
  t3 <- obj$enqueue(sin(pi / 2), depends_on = grp$task_ids)
  ## t3 has not been added to main queue yet
  expect_equal(obj$queue_list(), c(t, t2))
  expect_equal(unname(obj$task_status(t)), "PENDING")
  expect_equal(unname(obj$task_status(t2)), "PENDING")
  expect_equal(unname(obj$task_status(grp$task_ids)),
               c("DEFERRED", "DEFERRED"))
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")

  ## Original dependencies are stored
  grp_id_1 <- grp$task_ids[[1]]
  grp_id_2 <- grp$task_ids[[2]]
  original_grp_dep_id_1 <- rrq_key_task_depends_up_original(
    obj$queue_id, grp_id_1)
  expect_setequal(obj$con$SMEMBERS(original_grp_dep_id_1), c(t, t2))
  original_grp_dep_id_2 <- rrq_key_task_depends_up_original(
    obj$queue_id, grp_id_2)
  expect_setequal(obj$con$SMEMBERS(original_grp_dep_id_2), c(t, t2))
  original_deps_keys_t3 <- rrq_key_task_depends_up_original(
    obj$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(original_deps_keys_t3), grp$task_ids)

  ## Pending dependencies are stored
  grp_dep_id_1 <- rrq_key_task_depends_up(obj$queue_id, grp_id_1)
  expect_setequal(obj$con$SMEMBERS(grp_dep_id_1), c(t, t2))
  grp_dep_id_2 <- rrq_key_task_depends_up(obj$queue_id, grp_id_2)
  expect_setequal(obj$con$SMEMBERS(grp_dep_id_2), c(t, t2))
  dependency_keys_t3 <- rrq_key_task_depends_up(obj$queue_id, t3)
  expect_setequal(obj$con$SMEMBERS(dependency_keys_t3), grp$task_ids)

  ## Inverse depends_on relationship is stored
  dependent_keys_t <- rrq_key_task_depends_down(obj$queue_id, t)
  for (key in dependent_keys_t) {
    expect_setequal(obj$con$SMEMBERS(key), grp$task_ids)
  }
  dependent_keys_t2 <- rrq_key_task_depends_down(obj$queue_id, t2)
  for (key in dependent_keys_t2) {
    expect_setequal(obj$con$SMEMBERS(key), grp$task_ids)
  }
  grp_dependents_1 <- rrq_key_task_depends_down(obj$queue_id, grp_id_1)
  for (key in grp_dependents_1) {
    expect_setequal(obj$con$SMEMBERS(key), t3)
  }
  grp_dependents_2 <- rrq_key_task_depends_down(obj$queue_id, grp_id_2)
  for (key in grp_dependents_2) {
    expect_setequal(obj$con$SMEMBERS(key), t3)
  }

  w$step(TRUE)
  obj$task_wait(t, 2)
  expect_equal(unname(obj$task_status(t)), "COMPLETE")
  expect_equal(unname(obj$task_status(t2)), "PENDING")
  expect_equal(unname(obj$task_status(grp$task_ids)),
               c("DEFERRED", "DEFERRED"))
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")

  w$step(TRUE)
  obj$task_wait(t2, 2)
  expect_equal(unname(obj$task_status(t2)), "COMPLETE")
  expect_equal(unname(obj$task_status(grp$task_ids)),
               c("PENDING", "PENDING"))
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")
  queue <- obj$queue_list()
  expect_setequal(queue, grp$task_ids)

  w$step(TRUE)
  obj$task_wait(queue[1], 2)
  expect_setequal(obj$task_status(grp$task_ids), c("COMPLETE", "PENDING"))
  expect_equal(unname(obj$task_status(t3)), "DEFERRED")
  expect_equal(obj$queue_list(), queue[2])

  w$step(TRUE)
  obj$task_wait(queue[2], 2)
  expect_equal(unname(obj$task_status(grp$task_ids)),
               c("COMPLETE", "COMPLETE"))
  expect_equal(unname(obj$task_status(t3)), "PENDING")
  expect_equal(obj$queue_list(), t3)

  w$step(TRUE)
  obj$task_wait(t3, 2)
  expect_equal(unname(obj$task_status(t3)), "COMPLETE")
  expect_equal(obj$queue_list(), character(0))
})


test_that("Can offload storage for bulk tasks", {
  skip_if_no_redis()

  path <- tempfile()

  obj <- test_rrq(store_max_size = 100, offload_path = path)
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)

  a <- 10
  b <- runif(20)
  t <- obj$lapply(1:10, function(a, b) sum(b) / a, b, timeout_task_wait = 0)

  w <- test_worker_blocking(obj)
  w$loop(TRUE)

  res <- obj$bulk_wait(t)
  expect_equal(res, as.list(sum(b) / (1:10)))
})


test_that("by default, bulk fetch does not throw", {
  e <- new.env()
  sys.source("myfuns.R", e)
  obj <- test_rrq("myfuns.R")
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)

  w <- test_worker_blocking(obj)
  grp <- obj$lapply(seq(-1, 1), only_positive, envir = e, timeout_task_wait = 0)

  w$loop(TRUE)
  res <- obj$bulk_wait(grp)

  expect_s3_class(res[[1]], "rrq_task_error")
  expect_equal(res[[1]]$message, "x must be positive")
  expect_equal(res[[2]], 0)
  expect_equal(res[[3]], 1)
})


test_that("can throw on fetching bulk tasks", {
  e <- new.env()
  sys.source("myfuns.R", e)
  obj <- test_rrq("myfuns.R")
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)

  w <- test_worker_blocking(obj)
  grp <- obj$lapply(seq(-1, 1), only_positive, envir = e, timeout_task_wait = 0)

  w$loop(TRUE)

  err <- expect_error(obj$tasks_result(grp$task_ids, error = TRUE),
                      "1/3 tasks failed",
                      class = "rrq_task_error_group")
  expect_type(err$errors, "list")
  expect_length(err$errors, 1)
  expect_s3_class(err$errors[[1]], "rrq_task_error")
  expect_equal(err$errors[[1]], obj$task_result(grp$task_ids[[1]]))

  ## testthat adds this trace; remove it before the comparison below.
  err$trace <- NULL

  err2 <- tryCatch(obj$bulk_wait(grp, error = TRUE), error = identity)
  expect_s3_class(err2, "rrq_task_error_group")
  expect_null(err2$trace)
  expect_equal(err2, err)
})


test_that("can summarise fetching many failed bulk tasks", {
  e <- new.env()
  sys.source("myfuns.R", e)
  obj <- test_rrq("myfuns.R")
  cfg <- rrq_worker_config(verbose = FALSE, timeout_idle = -1, poll_queue = 1)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg, overwrite = TRUE)

  w <- test_worker_blocking(obj)
  grp <- obj$lapply(seq(-10, 0), only_positive, envir = e,
                    timeout_task_wait = 0)

  w$loop(TRUE)

  err <- expect_error(obj$tasks_result(grp$task_ids, error = TRUE),
                      "10/11 tasks failed",
                      class = "rrq_task_error_group")
  expect_equal(err$message,
               paste(c("10/11 tasks failed:",
                       rep("    - x must be positive", 4),
                       "    - ..."), collapse = "\n"))
  expect_type(err$errors, "list")
  expect_length(err$errors, 10)
  expect_s3_class(err$errors[[4]], "rrq_task_error")
  expect_equal(err$errors[[4]], obj$task_result(grp$task_ids[[4]]))
})


test_that("Set completion keys", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  grp <- obj$lapply_(1:4, sin, dots = list(base = 2), timeout_task_wait = 0)
  for (i in seq_along(grp$task_ids)) {
    w$step(immediate = TRUE)
  }

  expect_equal(obj$con$LRANGE(grp$key_complete, 0, -1),
               as.list(grp$task_ids))
  expect_equal(lapply(rrq_key_task_complete(obj$queue_id, grp$task_ids),
                      function(k) list_to_character(obj$con$LRANGE(k, 0, -1))),
               as.list(grp$task_ids))
})


test_that("allow referring to locals in bulk", {
  obj <- test_rrq("funs-bulk.R")
  obj$envir(rrq::rrq_envir(sources = c("funs-bulk.R")))

  ## We can't easily recreate the previous error (#98, #99) with a
  ## blocking worker because:
  ##
  ## * if sourcing functions into an environment we *do* capture the
  ##   enclosing environment and the underlying variable so the
  ##   restored functions are just fine
  ## * if sourcing into the global environment, then the worker can
  ##   see everything and the restored function works fine.
  ##
  ## However, if we delete the objects from the global environment
  ## just before running we get it to fail nicely:
  env <- globalenv()
  sys.source("funs-bulk.R", env)
  g <- obj$lapply(1, f, timeout_task_wait = 0, envir = env)
  rm("f", "a", envir = env)

  w <- test_worker_blocking(obj)
  w$step(TRUE)

  id <- g$task_ids
  expect_equal(unname(obj$task_status(id)), TASK_COMPLETE)
  expect_equal(obj$task_result(id), 11)
})
