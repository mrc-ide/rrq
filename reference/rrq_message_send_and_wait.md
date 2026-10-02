# Send a message and wait for response

Send a message and wait for responses. This is a helper function around
[`rrq_message_send()`](https://mrc-ide.github.io/rrq/reference/rrq_message_send.md)
and
[`rrq_message_get_response()`](https://mrc-ide.github.io/rrq/reference/rrq_message_get_response.md).

## Usage

``` r
rrq_message_send_and_wait(
  command,
  args = NULL,
  worker_ids = NULL,
  named = TRUE,
  delete = TRUE,
  timeout = 600,
  time_poll = 0.05,
  progress = NULL,
  controller = NULL
)
```

## Arguments

- command:

  A command, such as `PING`, `PAUSE`; see the Messages section of the
  Details for al messages.

- args:

  Arguments to the command, if supported

- worker_ids:

  Optional vector of worker ids to send the message to. If `NULL` then
  the message will be sent to all active workers.

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

## Value

The message response

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
rrq_message_send_and_wait("PING", controller = obj)
}
```
