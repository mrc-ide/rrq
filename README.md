---
output: github_document
---

<!-- README.md is generated from README.Rmd. Please edit that file -->



# rrq

<!-- badges: start -->
[![Project Status: WIP - Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](http://www.repostatus.org/badges/latest/wip.svg)](http://www.repostatus.org/#wip)
[![R build status](https://github.com/mrc-ide/rrq/workflows/R-CMD-check/badge.svg)](https://github.com/mrc-ide/rrq/actions)
[![codecov.io](https://codecov.io/github/mrc-ide/rrq/coverage.svg?branch=master)](https://codecov.io/github/mrc-ide/rrq?branch=master)
[![CodeFactor](https://www.codefactor.io/repository/github/mrc-ide/rrq/badge)](https://www.codefactor.io/repository/github/mrc-ide/rrq)
<!-- badges: end -->

Task queues for R, implemented using Redis.

## Getting started

Create an `rrq_controller` object


```r
obj <- rrq::rrq_controller("rrq:readme")
```

Submit work to the queue:


```r
t <- obj$enqueue(runif(10))
t
#> [1] "53a2e58afc836ff75a0abab53f75e85f"
```

Query task process:


```r
obj$task_status(t)
#> 53a2e58afc836ff75a0abab53f75e85f
#>                        "PENDING"
```

Run tasks on workers in the background


```r
rrq::worker_spawn(obj)
#> Spawning 1 worker with prefix nearby_ammonite
#> [1] "nearby_ammonite_1"
```

Collect task results when complete


```r
obj$task_wait(t)
#>  [1] 0.9499495 0.3754099 0.4028146 0.6498432 0.1732155 0.6069491 0.6379786
#>  [8] 0.8193451 0.1927368 0.8221052
```

Query what workers have done


```r
obj$worker_log_tail(n = Inf)
#>           worker_id       time       command                          message
#> 1 nearby_ammonite_1 1623142169         ALIVE
#> 2 nearby_ammonite_1 1623142169    TASK_START 53a2e58afc836ff75a0abab53f75e85f
#> 3 nearby_ammonite_1 1623142169 TASK_COMPLETE 53a2e58afc836ff75a0abab53f75e85f
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
