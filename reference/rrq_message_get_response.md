# Get message response

Get response to messages, waiting until the message has been responded
to.

## Usage

``` r
rrq_message_get_response(
  message_id,
  worker_ids = NULL,
  named = TRUE,
  delete = FALSE,
  timeout = 0,
  time_poll = 0.5,
  progress = NULL,
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

  Logical, indicating if the return value should be named by worker id.

- delete:

  Logical, indicating if messages should be deleted after retrieval

- timeout:

  Integer, representing seconds to wait until the response has been
  received. An error will be thrown if a response has not been received
  in this time.

- time_poll:

  If `timeout` is greater than zero, this is the polling interval used
  between redis calls. Increasing this reduces network load but
  increases the time that may be waited for.

- progress:

  Optional logical indicating if a progress bar should be displayed. If
  `NULL` we fall back on the value of the global option `rrq.progress`,
  and if that is unset display a progress bar if in an interactive
  session.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
id <- rrq_message_send("PING", controller = obj)
rrq_message_get_response(id, timeout = 5, controller = obj)
}
```
