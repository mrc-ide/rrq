context("redis utilities")

test_that("scan expire", {
  skip_if_no_redis()
  prefix1 <- sprintf("rrq:%s", ids::random_id())
  prefix2 <- sprintf("rrq:%s", ids::random_id())
  keys1 <- sprintf("%s:%s", prefix1, letters)
  keys2 <- sprintf("%s:%s", prefix2, letters)
  pat1 <- sprintf("%s:*", prefix1)
  pat2 <- sprintf("%s:*", prefix2)

  con <- redux::hiredis()
  con$MSET(keys1, letters)
  con$MSET(keys2, letters)

  expect_equal(scan_expire(con, pat1, 100), 26)
  expect_gt(con$TTL(keys1[[1]]), 0)
  expect_lte(con$TTL(keys1[[1]]), 100)

  expect_equal(scan_expire(con, pat2, 1), 26)
  Sys.sleep(1)
  expect_null(con$GET(keys2[[1]]))

  con$DEL(keys1)
})


test_that("blpop (immediate)", {
  con <- redux::hiredis()
  keys <- sprintf("rrq:%s:%d", ids::random_id(), 1:3)
  on.exit(con$DEL(keys))

  expect_null(blpop(con, keys, 0, TRUE))

  con$RPUSH(keys[[1]], "1")
  con$RPUSH(keys[[2]], "2-1")
  con$RPUSH(keys[[2]], "2-2")
  con$RPUSH(keys[[3]], "3")

  expect_equal(blpop(con, keys, 0, TRUE),
               list(keys[[1]], "1"))
  expect_equal(blpop(con, keys, 0, TRUE),
               list(keys[[2]], "2-1"))
  expect_equal(blpop(con, keys, 0, TRUE),
               list(keys[[2]], "2-2"))
  expect_equal(blpop(con, keys, 0, TRUE),
               list(keys[[3]], "3"))
  expect_null(blpop(con, keys, 0, TRUE))
})
