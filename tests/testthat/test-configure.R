test_that("Can set and retrieve a configuration", {
  skip_if_no_redis()
  name <- sprintf("rrq:%s", ids::random_id())
  on.exit(test_hiredis()$DEL(rrq_keys(name)$configuration))
  config <- rrq_configure(name, store_max_size = 100)
  expect_equal(config, list(store_max_size = 100, offload_path = NULL))
  expect_equal(rrq_configure_read(test_hiredis(), rrq_keys(name)),
               config)
})


test_that("Reading default configuration sets it", {
  skip_if_no_redis()
  con <- test_hiredis()
  name <- sprintf("rrq:%s", ids::random_id())
  on.exit(test_hiredis()$DEL(rrq_keys(name)$configuration))
  keys <- rrq_keys(name)
  config <- rrq_configure_read(con, keys)
  expect_equal(config, list(store_max_size = Inf, offload_path = NULL))
  expect_equal(con$EXISTS(keys$configuration), 1)
  expect_equal(bin_to_object(con$GET(keys$configuration)), config)
})


test_that("Can't set a conflicting configuration", {
  skip_if_no_redis()
  name <- sprintf("rrq:%s", ids::random_id())
  on.exit(test_hiredis()$DEL(rrq_keys(name)$configuration))
  config <- rrq_configure(name, store_max_size = 100)
  expect_error(
    rrq_configure(name, store_max_size = 101),
    "Can't set configuration for queue '.+' as it already exists")
  expect_error(
    rrq_configure(name, store_max_size = Inf),
    "Can't set configuration for queue '.+' as it already exists")
  expect_equal(rrq_configure_read(test_hiredis(), rrq_keys(name)),
               config)
})


test_that("Can set an identical configuration", {
  skip_if_no_redis()
  name <- sprintf("rrq:%s", ids::random_id())
  on.exit(test_hiredis()$DEL(rrq_keys(name)$configuration))
  config1 <- rrq_configure(name, store_max_size = 100)
  config2 <- rrq_configure(name, store_max_size = 100)
  expect_identical(config1, config2)
})


test_that("Configuring an offload path is deprecated", {
  skip_if_no_redis()
  name <- sprintf("rrq:%s", ids::random_id())
  on.exit(test_hiredis()$DEL(rrq_keys(name)$configuration))

  offload_path <- tempfile()

  testthat::expect_warning({
    config <- rrq_configure(name, store_max_size = 100,
                            offload_path = offload_path)
  }, "The `offload_path` argument is deprecated.")

  expect_equal(config, list(store_max_size = 100, offload_path = offload_path))
  expect_equal(rrq_configure_read(test_hiredis(), rrq_keys(name)),
               config)
})


test_that("Check all arguments consumed", {
  skip_if_no_redis()
  name <- sprintf("rrq:%s", ids::random_id())
  expect_error(rrq_configure(name, max_size = 100),
               "Unconsumed dot arguments")
})
