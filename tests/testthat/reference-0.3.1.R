## This is the script used to generate reference data for migrating
## from 0.3.1 to 0.4.0 where we changed the storage model. This
## generates 3 different types of tasks - one with no objects, one
## with objects, and some with an anonymous function (via
## lapply). It's not actually possible to generate both anonymous
## function + objects with the current implementation I think due to a
## bug fixed in 0.4.0
rrq <- test_rrq()

rrq$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                       time_poll = 1, overwrite = TRUE)
w <- test_worker_blocking(rrq)

a <- 10
b <- 20
t1 <- rrq$enqueue(1 + 1)
t2 <- rrq$enqueue(a / b)
grp <- rrq$lapply_(1:3, function(x) sqrt(x), collect_timeout = 0)

value <- set_names(c(list(1 + 1, a / b), lapply(1:3, sqrt)),
                   c(t1, t2, grp$task_ids))

w$loop(TRUE)

keys <- redux::scan_find(rrq$con, paste0(rrq$queue_id, "*"))

## We can't use DUMP here because that won't survive different
## versions.
data <- list(queue_id = rrq$queue_id,
             data = set_names(lapply(keys, redis_dump, rrq$con), keys),
             tasks = value)

dir.create("migrate", FALSE, TRUE)
saveRDS(data, "migrate/0.3.1.rds", version = 2)
saveRDS(data, "~/Documents/src/rrq/tests/testthat/migrate/0.3.1.rds", version = 2)
