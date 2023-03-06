test_that("set schema version if unknown", {
  con <- test_hiredis()
  keys <- rrq_keys(sprintf("rrq:%s", ids::random_id()))
  on.exit(rrq_clean(con, keys$queue_id))
  expect_null(rrq_version_check(con, keys))
  expect_equal(con$GET(keys$version), rrq_schema_version)
  expect_null(rrq_version_check(con, keys))
})


test_that("Error if schema version incompatible", {
  con <- test_hiredis()
  keys <- rrq_keys(sprintf("rrq:%s", ids::random_id()))
  on.exit(rrq_clean(con, keys$queue_id))
  con$SET(keys$version, "0.1.0")
  expect_error(
    rrq_version_check(con, keys),
    "rrq schema version is '0.1.0' but you are using '0.4.0'; please migrate")
  con$SET(keys$version, "0.9.0")
  expect_error(
    rrq_version_check(con, keys),
    "rrq schema version is '0.9.0' but you are using '0.4.0'; please upgrade")
})
