# Save worker configuration

Save a worker configuration, which can be used to start workers with a
set of options with the cli. These correspond to arguments to
[rrq_worker](https://mrc-ide.github.io/rrq/reference/rrq_worker.md).
**This function will be renamed soon**

## Usage

``` r
rrq_worker_config_save(name, config, overwrite = TRUE, controller = NULL)
```

## Arguments

- name:

  Name for this configuration

- config:

  A worker configuration, created by
  [`rrq_worker_config()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_config.md)

- overwrite:

  Logical, indicating if an existing configuration with this `name`
  should be overwritten if it exists. If `FALSE`, then the configuration
  is not updated, even if it differs from the version currently saved.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

Invisibly, a boolean indicating if the configuration was updated.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

cfg <- rrq_worker_config("fast")
rrq_worker_config_save("use-fast", cfg, controller = obj)
rrq_worker_config_list(controller = obj)
}
```
