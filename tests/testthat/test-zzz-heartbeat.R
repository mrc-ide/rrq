context("heartbeat")

test_that("basic interaction with heartbeat works", {
  skip_if_no_redis()
  config <- redux::redis_config()
  key <- sprintf("rrq:heartbeat:basic:%s", ids::random_id())
  period <- 1
  expire <- 2
  obj <- heartbeat$new(key, period, expire = expire, start = FALSE)
  expect_is(obj, "heartbeat")
  expect_is(obj, "R6")

  con <- redux::hiredis()
  expect_equal(con$EXISTS(key), 0)
  on.exit(con$DEL(key))
  expect_false(obj$is_running())

  expect_error(obj$stop(),
               "Heartbeat not running on key")

  obj$start()
  wait_timeout("Key not available in time", 5, function() con$EXISTS(key) == 0)

  expect_equal(con$EXISTS(key), 1)
  expect_equal(con$GET(key), as.character(expire))
  ttl <- con$TTL(key)

  expect_gt(ttl, period - expire)
  expect_lte(ttl, expire)
  expect_true(obj$is_running())

  expect_error(obj$start(), "Already running on key")
  expect_true(obj$is_running())

  obj$stop()
  expect_false(obj$is_running())
  expect_equal(con$EXISTS(key), 0)
})


test_that("Garbage collection", {
  skip_if_no_redis()
  key <- sprintf("rrq:heartbeat:gc%s", ids::random_id())
  period <- 1
  expire <- 2
  con <- redux::hiredis()

  obj <- heartbeat$new(key, period, expire = expire)
  expect_equal(con$EXISTS(key), 1)
  expect_true(obj$is_running())

  rm(obj)
  gc()

  ## We might have to wait up to 'expire' seconds for this key to
  ## disappear. We could add an attempt to clean up into the finaliser
  ## but that will cause stalls on garbage collection, which is rude.
  wait_timeout("Key not expired in time", expire, function()
    con$EXISTS(key) == 1)
  expect_equal(con$EXISTS(key), 0)
})


test_that("Kill process", {
  skip_if_no_redis()

  key <- sprintf("rrq:heartbeat:kill:%s", ids::random_id())

  px <- callr::r_bg(function(key) {
    config <- redux::redis_config()
    obj <- heartbeat$new(key, 1, 2, config = config)
    Sys.sleep(120)
  }, list(key = key), package = "rrq")
  pid <- px$get_pid()

  con <- redux::hiredis()
  wait_timeout("Process did not start up in time", 5, function()
    con$EXISTS(key) == 0 && px$is_alive(), poll = 0.2)

  heartbeat_send_signal(con, key, tools::SIGTERM)

  wait_timeout("Process did stop in time", 5, function()
    px$is_alive(), poll = 0.2)
  expect_false(px$is_alive())
  expect_equal(con$EXISTS(key), 0)
})


test_that("Interrupt process", {
  skip_if_no_redis()
  skip_on_os("windows")

  key <- sprintf("rrq:heartbeat:interrupt:%s", ids::random_id())
  path <- tempfile()

  px <- callr::r_bg(function(key, path) {
    config <- redux::redis_config()
    obj <- heartbeat$new(key, 1, 2, config = config)
    writeLines("1", path)
    tryCatch(
      Sys.sleep(120),
      interrupt = function(e) NULL)
    writeLines("2", path)
    Sys.sleep(120)
  }, list(key = key, path = path), package = "rrq")

  con <- redux::hiredis()
  wait_timeout("Process did not start up in time", 5, function()
    con$EXISTS(key) == 0 && px$is_alive(), poll = 0.2)

  expect_equal(con$EXISTS(key), 1)
  expect_true(px$is_alive())

  wait_timeout("File did not update in time", 5, function()
    !file.exists(path), poll = 0.1)
  expect_equal(readLines(path), "1")

  heartbeat_send_signal(con, key, tools::SIGINT)

  wait_timeout("File did not update in time", 5, function()
    readLines(path) == "1" && px$is_alive(), poll = 0.1)

  expect_equal(readLines(path), "2")
  expect_true(px$is_alive())
  px$signal(tools::SIGTERM
})


test_that("dying process", {
  skip_if_no_redis()

  key <- sprintf("rrq:heartbeat:die:%s", ids::random_id())

  px <- callr::r_bg(function(key) {
    config <- redux::redis_config()
    obj <- heartbeat$new(key, 1, 2, config = config)
    Sys.sleep(120)
  }, list(key = key), package = "rrq")

  con <- redux::hiredis()
  wait_timeout("Process did not start up in time", 5, function()
    con$EXISTS(key) == 0 && px$is_alive(), poll = 0.2)

  expect_equal(con$EXISTS(key), 1)
  expect_true(px$is_alive())

  expect_equal(con$EXISTS(key), 1)
  px$signal(tools::SIGTERM)
  wait_timeout("Process did not die in time", 5, px$is_alive)
  expect_equal(con$EXISTS(key), 1)
  Sys.sleep(2) # expire
  expect_equal(con$EXISTS(key), 0)
})


test_that("invalid times", {
  key <- sprintf("rrq:heartbeat:confail:%s", ids::random_id())
  period <- 10
  expect_error(heartbeat$new(key, period, expire = period),
               "expire must be longer than period")
  expect_error(heartbeat$new(key, period, expire = period - 1),
               "expire must be longer than period")
})


test_that("print", {
  skip_if_no_redis()
  key <- sprintf("rrq:heartbeat:print:%s", ids::random_id())
  period <- 1
  obj <- heartbeat$new(key, period, start = FALSE)
  str <- capture.output(tmp <- print(obj))
  expect_identical(tmp, obj)
  expect_match(str, "<heartbeat>", fixed = TRUE, all = FALSE)
  expect_match(str, "running: false", fixed = TRUE, all = FALSE)
})


## Currently failing on R-devel because of changes that restrict
## assignment of the traceback (currently used by callr); see
## https://github.com/r-lib/callr/issues/196
test_that("handle startup failure", {
  skip_if_no_redis()
  skip_if(getRversion() > numeric_version("4.0.5"))
  config <- redux::redis_config()
  key <- sprintf("rrq:heartbeat:basic:%s", ids::random_id())
  period <- 5
  expire <- 10
  obj <- heartbeat$new(key, period, expire = expire, start = FALSE)

  ## Then we'll break the config:
  private <- environment(obj$initialize)$private
  private$value <- NULL

  ## This error comes from redux
  expect_error(obj$start(), "value must be a scalar")

  expect_false(obj$is_running())
  expect_equal(redux::hiredis()$EXISTS(key), 0)
})
