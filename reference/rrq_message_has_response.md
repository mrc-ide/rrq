# Detect if message has response

Detect if a response is available for a message

## Usage

``` r
rrq_message_has_response(
  message_id,
  worker_ids = NULL,
  named = TRUE,
  controller = NULL
)
```

## Arguments

- message_id:

  The message id

- worker_ids:

  Optional vector of worker ids. If `NULL` then all active workers are
  used (note that this may differ to the set of workers that the message
  was sent to!)

- named:

  Logical, indicating if the return vector should be named

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A logical vector, possibly named (depending on the `named` argument)

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

id <- rrq_message_send("PING", controller = obj)
rrq_message_has_response(id, controller = obj)
rrq_message_get_response(id, timeout = 5, controller = obj)
rrq_message_has_response(id, controller = obj)
}
```
