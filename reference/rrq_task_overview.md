# High level task overview

Provide a high level overview of task statuses for a set of task ids,
being the count in major categories of `PENDING`, `RUNNING`, `COMPLETE`,
`ERROR`, `CANCELLED`, `DIED`, `TIMEOUT`, `IMPOSSIBLE`, `DEFERRED` and
`MOVED`.

## Usage

``` r
rrq_task_overview(task_ids = NULL, controller = NULL)
```

## Arguments

- task_ids:

  Optional character vector of task ids for which you would like the
  overview. If not given (or `NULL`) then the status of all task ids
  known to this rrq controller is used (this might be fairly costly).

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A list with names corresponding to possible task status levels and
values being the number of tasks in that state.

## Examples

``` r

obj <- rrq_controller("rrq:example")

ids <- rrq_task_list(controller = obj)
t(as.data.frame(rrq_task_overview(ids, controller = obj)))
#>            [,1]
#> PENDING       3
#> RUNNING       0
#> COMPLETE      0
#> ERROR         0
#> CANCELLED     0
#> DIED          0
#> TIMEOUT       0
#> IMPOSSIBLE    0
#> DEFERRED      0
#> MOVED         0
```
