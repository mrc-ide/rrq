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
obj <- rrq_controller("rrq:readme")
rrq_default_controller_set(obj)
```

Submit work to the queue:


```r
t <- rrq_task_create_expr(runif(10))
t
#> [1] "0e339d67495c33cde07d5c9784b24f31"
```

Query task process:


```r
rrq_task_status(t)
#> [1] "PENDING"
```

Run tasks on workers in the background


```r
rrq_worker_spawn()
#> ℹ Spawning 1 worker with prefix 'bossy_americanredsquirrel'
#> <rrq_worker_manager>
#>   Public:
#>     clone: function (deep = FALSE)
#>     id: bossy_americanredsquirrel_1
#>     initialize: function (controller, n, logdir = NULL, name_config = "localhost",
#>     is_alive: function (worker_id = NULL)
#>     kill: function (worker_id = NULL)
#>     logs: function (worker_id)
#>     stop: function (worker_id = NULL, ...)
#>     wait_alive: function (timeout, time_poll = 0.2, progress = NULL)
#>   Private:
#>     check_worker_id: function (worker_id)
#>     controller: rrq_controller
#>     logfile: /tmp/RtmpReX932/file920c215c4c48c/bossy_americanredsquir ...
#>     process: list
#>     worker_id_base: bossy_americanredsquirrel
```

Wait for tasks to complete


```r
rrq_task_wait(t)
#> [1] TRUE
```

Retrieve results from a task


```r
rrq_task_result(t)
#>  [1] 0.5660174 0.7272429 0.3753210 0.4081318 0.5124689 0.3461631 0.3997216
#>  [8] 0.7212593 0.9927757 0.3032974
```

Query what workers have done


```r
rrq_worker_log_tail(n = Inf)
#>                     worker_id child       time       command
#> 1 bossy_americanredsquirrel_1    NA 1713860205         ALIVE
#> 2 bossy_americanredsquirrel_1    NA 1713860205         ENVIR
#> 3 bossy_americanredsquirrel_1    NA 1713860205         QUEUE
#> 4 bossy_americanredsquirrel_1    NA 1713860205    TASK_START
#> 5 bossy_americanredsquirrel_1    NA 1713860205 TASK_COMPLETE
#>                            message
#> 1
#> 2                              new
#> 3                          default
#> 4 0e339d67495c33cde07d5c9784b24f31
#> 5 0e339d67495c33cde07d5c9784b24f31
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
