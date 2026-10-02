# Send message to workers

Send a message to workers. Sending a message returns a message id, which
can be used to poll for a response with the other `rrq_message_*`
functions. See
[`vignette("messages")`](https://mrc-ide.github.io/rrq/articles/messages.md)
for details for the messaging interface.

## Usage

``` r
rrq_message_send(command, args = NULL, worker_ids = NULL, controller = NULL)
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

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

Invisibly, a single identifier

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

id <- rrq_message_send("PING", controller = obj)
rrq_message_get_response(id, timeout = 5, controller = obj)
}
```
