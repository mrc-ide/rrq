# List exited workers

Returns the ids of workers known to have exited

## Usage

``` r
rrq_worker_list_exited(controller = NULL)
```

## Arguments

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A character vector of worker names

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

# At this point you might have an exited worker, depending on
# which examples have been run so far!
rrq_worker_list_exited(controller = obj)

# Spawn a new worker so that we can stop it:
w <- rrq_worker_spawn(1, controller = obj)$id
w$id
# Stop this worker and see that it appears in the list of exited
# workers:
rrq_worker_stop(w$id, controller = obj)
rrq_worker_list_exited(controller = obj)

# We can delete this exited worker:
rrq_worker_delete_exited(w$id, controller = obj)

# After this, it is no longer listed as exited:
rrq_worker_list_exited(controller = obj)
}
```
