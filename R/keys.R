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

       controller    = sprintf("%s:controller",     queue),

       db_prefix     = sprintf("%s:db:",            queue),
       envir         = sprintf("%s:envir",          queue),

       worker_config = sprintf("%s:worker:config",  queue),
       worker_name   = sprintf("%s:worker:name",    queue),
       worker_status = sprintf("%s:worker:status",  queue),
       worker_task   = sprintf("%s:worker:task",    queue),
       worker_info   = sprintf("%s:worker:info",    queue),
       worker_expect = sprintf("%s:worker:expect",  queue),
       ## The process log, not the worker log - possibly empty
       worker_process = sprintf("%s:worker:process", queue),

       queue         = sprintf("%s:queue",          queue),

       task_expr     = sprintf("%s:task:expr",      queue),
       task_status   = sprintf("%s:task:status",    queue),
       task_worker   = sprintf("%s:task:worker",    queue),
       task_result   = sprintf("%s:task:result",    queue),
       task_complete = sprintf("%s:task:complete",  queue))
}

rrq_keys_worker <- function(queue, worker) {
  list(worker_message   = rrq_key_worker_message(queue, worker),
       worker_response  = rrq_key_worker_response(queue, worker),
       worker_log       = rrq_key_worker_log(queue, worker),
       worker_heartbeat = rrq_key_worker_heartbeat(queue, worker))
}

## Special key for worker-specific commands to be published to.
rrq_key_worker_message <- function(queue, worker) {
  sprintf("%s:worker:%s:message", queue, worker)
}
rrq_key_worker_response <- function(queue, worker) {
  sprintf("%s:worker:%s:response", queue, worker)
}
rrq_key_worker_log <- function(queue, worker) {
  sprintf("%s:worker:%s:log", queue, worker)
}
rrq_key_worker_heartbeat <- function(queue, worker) {
  sprintf("%s:worker:%s:heartbeat", queue, worker)
}

## Randomly generated keys:
rrq_key_task_complete <- function(queue) {
  sprintf("%s:tasks:complete:%s", queue, ids::random_id())
}

rrq_key_worker_alive <- function(queue_name) {
  sprintf("%s:worker:alive:%s", queue_name, ids::random_id())
}
