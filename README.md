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
#> [1] "fd08ef462c509e174338ca5920b13adf"
```

Query task process:


```r
rrq_task_status(t)
#> [1] "PENDING"
```

Run tasks on workers in the background


```r
rrq_worker_spawn()
#> ℹ Spawning 1 worker with prefix 'nonmetalliferous_jabiru'
#> <rrq_worker_manager>
#>   Public:
#>     clone: function (deep = FALSE)
#>     id: nonmetalliferous_jabiru_1
#>     initialize: function (controller, n, logdir = NULL, name_config = "localhost",
#>     is_alive: function (worker_id = NULL)
#>     kill: function (worker_id = NULL)
#>     logs: function (worker_id)
#>     stop: function (worker_id = NULL, ...)
#>     wait_alive: function (timeout, time_poll = 0.2, progress = NULL)
#>   Private:
#>     check_worker_id: function (worker_id)
#>     controller: rrq_controller
#>     logfile: /tmp/Rtmp4AMYNM/filee336a6e3e1369/nonmetalliferous_jabiru_1
#>     process: list
#>     worker_id_base: nonmetalliferous_jabiru
```

Wait for tasks to complete


```r
rrq_task_wait(t)
#> [1] TRUE
```

Retrieve results from a task


```r
rrq_task_result(t)
#>  [1] 0.044105073 0.151111529 0.047623996 0.936703515 0.719949653 0.519903127
#>  [7] 0.030698510 0.057218178 0.503331142 0.002863957
```

Query what workers have done


```r
rrq_worker_log_tail(n = Inf)
#>                   worker_id child       time       command
#> 1 nonmetalliferous_jabiru_1    NA 1721033895         ALIVE
#> 2 nonmetalliferous_jabiru_1    NA 1721033895         ENVIR
#> 3 nonmetalliferous_jabiru_1    NA 1721033895         QUEUE
#> 4 nonmetalliferous_jabiru_1    NA 1721033895    TASK_START
#> 5 nonmetalliferous_jabiru_1    NA 1721033895 TASK_COMPLETE
#>                            message
#> 1
#> 2                              new
#> 3                          default
#> 4 fd08ef462c509e174338ca5920b13adf
#> 5 fd08ef462c509e174338ca5920b13adf
```

For more information, see `vignette("rrq")`



## Installation

Install from the mrc-ide R universe package repository:

```r
install.packages(
  "rrq",
  repos = c("https://mrc-ide.r-universe.dev", "https://cloud.r-project.org"))
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

Alternatively, on Windows, a native (i.e., not depending on CygWin, MSys, or WSL) port of Redis 5.0.14.1 can be installed from [here](https://github.com/tporadowski/redis/releases), and will run out of the box.

## Testing the pkgdown site with examples

The documentation includes many executed code examples. To preview the documentation with the results of these, you'll need a Redis server running, and the rrq package installed. Then, in an R terminal:-

```
rrq::rrq_worker$new("rrq:example")$loop()
```

to run a worker. Then in another R terminal,


```
pkgdown::build_site()
```

and the pkgdown site will be built in the `docs/` folder.

## License

MIT © Imperial College of Science, Technology and Medicine
