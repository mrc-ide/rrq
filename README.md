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

Create an `rrq_controller` object


```r
obj <- rrq::rrq_controller$new("rrq:readme")
```

Submit work to the queue:


```r
t <- obj$enqueue(runif(10))
t
#> [1] "1604b9d2665fc935c5f432b0d7da4889"
```

Query task process:


```r
obj$task_status(t)
#> 1604b9d2665fc935c5f432b0d7da4889
#>                        "PENDING"
```

Run tasks on workers in the background


```r
rrq::rrq_worker_spawn(obj)
#> Spawning 1 worker with prefix hexagonal_xenurine
#> [1] "hexagonal_xenurine_1"
```

Collect task results when complete


```r
obj$task_wait(t)
#>  [1] 0.21934439 0.47012520 0.57837978 0.17550807 0.07085051 0.04189457
#>  [7] 0.43439891 0.79186554 0.82606661 0.86208847
```

Or try and retrieve them regardless of if they are complete


```r
obj$task_result(t)
#>  [1] 0.21934439 0.47012520 0.57837978 0.17550807 0.07085051 0.04189457
#>  [7] 0.43439891 0.79186554 0.82606661 0.86208847
```

Query what workers have done


```r
obj$worker_log_tail(n = Inf)
#>              worker_id       time       command
#> 1 hexagonal_xenurine_1 1678370725         ALIVE
#> 2 hexagonal_xenurine_1 1678370725    TASK_START
#> 3 hexagonal_xenurine_1 1678370726 TASK_COMPLETE
#>                            message
#> 1
#> 2 1604b9d2665fc935c5f432b0d7da4889
#> 3 1604b9d2665fc935c5f432b0d7da4889
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
