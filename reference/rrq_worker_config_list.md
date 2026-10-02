# List worker configurations

Return names of worker configurations saved by
[`rrq_worker_config_save()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_config_save.md)

## Usage

``` r
rrq_worker_config_list(controller = NULL)
```

## Arguments

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A character vector of names; these can be passed as the `name` argument
to
[`rrq_worker_config_read()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_config_read.md).

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

cfg <- rrq_worker_config("fast")
rrq_worker_config_save("use-fast", cfg, controller = obj)
rrq_worker_config_list(controller = obj)
}
```
