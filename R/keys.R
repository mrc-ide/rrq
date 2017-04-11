## TODO: The prefix will need some work to avoid collisions with other
## people.  Avoid the system RNG too - might need to go with openssl
## for real RNGs, or with uuids.
rrq_keys <- function(queue_name, worker_name = NULL) {
  if (is.null(worker_name)) {
    rrq_keys_queue(queue_name)
  } else {
    c(rrq_keys_queue(queue_name),
      rrq_keys_worker(queue_name, worker_name))
  }
}

rrq_keys_queue <- function(queue) {
  ## TODO: queue_name should not be used like this because it's not a
  ## key itself.
  list(queue_name    = queue,

       controller    = sprintf("rrq:%s:controller",     queue),

       worker_name   = sprintf("rrq:%s:worker:name",    queue),
       worker_status = sprintf("rrq:%s:worker:status",  queue),
       worker_task   = sprintf("rrq:%s:worker:task",    queue),
       worker_info   = sprintf("rrq:%s:worker:info",    queue),
       worker_expect = sprintf("rrq:%s:worker:expect",  queue),

       queue_rrq     = sprintf("rrq:%s:queue:rrq:id",   queue),
       queue_ctx     = sprintf("rrq:%s:queue:ctx:id",   queue),

       task_count    = sprintf("rrq:%s:task:count",     queue),
       task_expr     = sprintf("rrq:%s:task:expr",      queue),
       task_status   = sprintf("rrq:%s:task:status",    queue),
       task_worker   = sprintf("rrq:%s:task:worker",    queue),
       task_result   = sprintf("rrq:%s:task:result",    queue),
       task_complete = sprintf("rrq:%s:task:complete",  queue))
}

rrq_keys_worker <- function(queue, worker) {
  list(message   = rrq_key_worker_message(queue, worker),
       response  = rrq_key_worker_response(queue, worker),
       log       = rrq_key_worker_log(queue, worker),
       heartbeat = rrq_key_worker_heartbeat(queue, worker))
}

## Special key for worker-specific commands to be published to.
rrq_key_worker_message <- function(queue, worker) {
  sprintf("rrq:%s:worker:%s:message", queue, worker)
}
rrq_key_worker_response <- function(queue, worker) {
  sprintf("rrq:%s:worker:%s:response", queue, worker)
}
rrq_key_worker_log <- function(queue, worker) {
  sprintf("rrq:%s:worker:%s:log", queue, worker)
}
rrq_key_worker_heartbeat <- function(queue, worker) {
  sprintf("rrq:%s:worker:%s:heartbeat", queue, worker)
}

## Randomly generated keys:
rrq_key_task_complete <- function(queue) {
  sprintf("rrq:%s:tasks:complete:%s", queue, ids::random_id())
}

rrq_key_worker_alive <- function(queue_name) {
  sprintf("rrq:%s:worker:alive:%s", queue_name, ids::random_id())
}
