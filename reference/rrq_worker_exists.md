# Test if a worker exists

Test if a worker exists

## Usage

``` r
rrq_worker_exists(name, controller = NULL)
```

## Arguments

- name:

  Name of the worker

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A logical value

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
w <- rrq_worker_list(controller = obj)
rrq_worker_exists(w, controller = obj)
rrq_worker_exists("bob-the-builder", controller = obj)
}
```
