# Delete tasks

Delete one or more tasks

## Usage

``` r
rrq_task_delete(task_ids, check = TRUE, controller = NULL)
```

## Arguments

- task_ids:

  Vector of task ids to delete

- check:

  Logical indicating if we should check that the tasks are not running.
  Deleting running tasks is unlikely to result in desirable behaviour.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

Nothing, called for side effects only

## Examples

``` r
obj <- rrq_controller("rrq:example:delete")

ts <- rrq_task_create_bulk_call(sqrt, 1:10, controller = obj)
rrq_task_exists(ts, controller = obj)
#>  [1] TRUE TRUE TRUE TRUE TRUE TRUE TRUE TRUE TRUE TRUE

rrq_task_delete(ts[1:5], controller = obj)
rrq_task_exists(ts, controller = obj)
#>  [1] FALSE FALSE FALSE FALSE FALSE  TRUE  TRUE  TRUE  TRUE  TRUE

rrq_task_delete(ts, controller = obj)
rrq_task_exists(ts, controller = obj)
#>  [1] FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE FALSE
```
