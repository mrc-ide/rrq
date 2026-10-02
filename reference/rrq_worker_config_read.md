# Read worker configuration

Return the value of a of worker configuration saved by
[`rrq_worker_config_save()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_config_save.md)

## Usage

``` r
rrq_worker_config_read(name, timeout = 0, controller = NULL)
```

## Arguments

- name:

  Name of the configuration (see
  [`rrq_worker_config_list()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_config_list.md))

- timeout:

  Optionally, a timeout to wait for a worker configuration to appear.
  Generally you won't want to set this, but it can be used to block
  until a configuration becomes available.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

cfg <- rrq_worker_config("fast")
rrq_worker_config_save("use-fast", cfg, controller = obj)
rrq_worker_config_read("use-fast", controller = obj)
}
```
