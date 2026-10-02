# Return ids for messages with responses for a particular worker.

Return ids for messages with responses for a particular worker.

## Usage

``` r
rrq_message_response_ids(worker_id, controller = NULL)
```

## Arguments

- worker_id:

  The worker id

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A character vector of ids

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
w <- rrq_worker_list(controller = obj)
rrq_message_send("PING", controller = obj)
}
```
