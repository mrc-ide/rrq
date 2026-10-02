# Current task id for workers

Returns the task id that each worker is working on, if any.

## Usage

``` r
rrq_worker_task_id(worker_ids = NULL, controller = NULL)
```

## Arguments

- worker_ids:

  Optional vector of worker ids. If `NULL` then all active workers are
  used.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A character vector, `NA` where nothing is being worked on, otherwise
corresponding to a task id.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
rrq_worker_list(controller = obj)

# This example might be a bit racy: we need to run a task that
# sleeps, and then sleep a little bit for the task to be picked up
# by a worker.  Typically this happens very quickly but there are
# no guarantees.
t <- rrq_task_create_expr(Sys.sleep(1), controller = obj)
Sys.sleep(.2)
rrq_worker_task_id(controller = obj)

# You can always find out which worker did work on a task though:
rrq_task_info(t, controller = obj)$worker
}
```
