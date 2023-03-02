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
#> [1] "6a368aa506da1aabcd264ca859fa3322"
```

Query task process:


```r
obj$task_status(t)
#> 6a368aa506da1aabcd264ca859fa3322
#>                        "PENDING"
```

Run tasks on workers in the background


```r
rrq::worker_spawn(obj)
#> Spawning 1 worker with prefix endocrinous_anemonecrab
#> [1] "endocrinous_anemonecrab_1"
```

Collect task results when complete


```r
obj$task_wait(t)
#>  [1] 0.6429454 0.3498470 0.2804653 0.2975355 0.1308821 0.1520124 0.3212029
#>  [8] 0.1526649 0.1589430 0.3306704
```

Or try and retrieve them regardless of if they are complete


```r
obj$task_result(t)
#>  [1] 0.6429454 0.3498470 0.2804653 0.2975355 0.1308821 0.1520124 0.3212029
#>  [8] 0.1526649 0.1589430 0.3306704
```

Query what workers have done


```r
obj$worker_log_tail(n = Inf)
#>                   worker_id       time       command
#> 1 endocrinous_anemonecrab_1 1629120683         ALIVE
#> 2 endocrinous_anemonecrab_1 1629120683    TASK_START
#> 3 endocrinous_anemonecrab_1 1629120683 TASK_COMPLETE
#>                            message
#> 1
#> 2 6a368aa506da1aabcd264ca859fa3322
#> 3 6a368aa506da1aabcd264ca859fa3322
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
