<!-- README.md is generated from README.Rmd. Please edit that file -->



# rrq

<!-- badges: start -->
[![Project Status: WIP - Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![R-CMD-check](https://github.com/mrc-ide/rrq/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/mrc-ide/rrq/actions/workflows/R-CMD-check.yaml)
[![codecov.io](https://codecov.io/github/mrc-ide/rrq/coverage.svg?branch=master)](https://codecov.io/github/mrc-ide/rrq?branch=master)
[![CodeFactor](https://www.codefactor.io/repository/github/mrc-ide/rrq/badge)](https://www.codefactor.io/repository/github/mrc-ide/rrq)
<!-- badges: end -->

Task queues for R, implemented using Redis.

## Getting started


```r
library(rrq)
```

Create an `rrq_controller` object


```r
obj <- rrq_controller2("rrq:readme")
rrq_default_controller_set(obj)
```

Submit work to the queue:


```r
t <- rrq_task_create_expr(runif(10))
t
#> [1] "0b6ae3ae76e1cfab43a8ad4d41b7c4cd"
```

Query task process:


```r
rrq_task_status(t)
#> [1] "PENDING"
```

Run tasks on workers in the background


```r
rrq_worker_spawn2()
#> ℹ Spawning 1 worker with prefix 'pepperish_zebralongwingbutterfly'
#> <rrq_worker_manager>
#>   Public:
#>     clone: function (deep = FALSE)
#>     id: pepperish_zebralongwingbutterfly_1
#>     initialize: function (controller, n, logdir = NULL, name_config = "localhost",
#>     is_alive: function (worker_id = NULL)
#>     kill: function (worker_id = NULL)
#>     logs: function (worker_id)
#>     stop: function (worker_id = NULL, ...)
#>     wait_alive: function (timeout, time_poll = 0.2, progress = NULL)
#>   Private:
#>     check_worker_id: function (worker_id)
#>     controller: rrq_controller2
#>     logfile: /tmp/Rtmp3k3m8o/file183821cbeb985/pepperish_zebralongwin ...
#>     process: list
#>     worker_id_base: pepperish_zebralongwingbutterfly
```

Wait for tasks to complete


```r
rrq_task_wait(t)
#> [1] TRUE
```

Retrieve results from a task


```r
rrq_task_result(t)
#>  [1] 0.88813000 0.69115977 0.58344801 0.49533313 0.23230984 0.94035381
#>  [7] 0.06302391 0.75492762 0.69768060 0.31522964
```

Query what workers have done


```r
rrq_worker_log_tail(n = Inf)
#>                            worker_id child       time       command
#> 1 pepperish_zebralongwingbutterfly_1    NA 1713530454         ALIVE
#> 2 pepperish_zebralongwingbutterfly_1    NA 1713530454         ENVIR
#> 3 pepperish_zebralongwingbutterfly_1    NA 1713530454         QUEUE
#> 4 pepperish_zebralongwingbutterfly_1    NA 1713530454    TASK_START
#> 5 pepperish_zebralongwingbutterfly_1    NA 1713530454 TASK_COMPLETE
#> 6 pepperish_zebralongwingbutterfly_1    NA 1713530454    TASK_START
#> 7 pepperish_zebralongwingbutterfly_1    NA 1713530454 TASK_COMPLETE
#>                            message
#> 1
#> 2                              new
#> 3                          default
#> 4 a7f89f9e729d693de669bb3eca5f6710
#> 5 a7f89f9e729d693de669bb3eca5f6710
#> 6 0b6ae3ae76e1cfab43a8ad4d41b7c4cd
#> 7 0b6ae3ae76e1cfab43a8ad4d41b7c4cd
```

For more information, see `vignette("rrq")`



## Installation

Install from the mrc-ide package repository:

```r
drat:::add("mrc-ide")
install.packages("rrq")
```

Alternatively, install with `remotes`:

```r
remotes::install_github("mrc-ide/rrq", upgrade = FALSE)
```

## Testing

To test, we need a redis server that can be automatically connected to using the `redux` defaults.  This is satisfied if you have an unauthenticated redis server running on localhost, otherwise you should update the environment variable `REDIS_URL` to point at a redis server.  Do not use a production server, as the package will create and delete a lot of keys.

A suitable redis server can be started using docker with

```
./scripts/redis start
```

(and stopped with `./scripts/redis stop`)

## License

MIT © Imperial College of Science, Technology and Medicine
