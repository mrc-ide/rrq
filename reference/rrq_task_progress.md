# Fetch task progress information

Retrieve task progress, if set. This will be `NULL` if progress has
never been registered, otherwise whatever value was set - can be an
arbitrary R object.

## Usage

``` r
rrq_task_progress(task_id, controller = NULL)
```

## Arguments

- task_id:

  A single task id for which the progress is wanted.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

Any set progress object
