## TODO: The prefix will need some work to avoid collisions with other
## people.  Avoid the system RNG too - might need to go with openssl
## for real RNGs, or with uuids.
rrq_keys <- function(queue_id, worker_name = NULL) {
  if (is.null(worker_name)) {
    rrq_keys_common(queue_id)
  } else {
    c(rrq_keys_common(queue_id),
      rrq_keys_worker(queue_id, worker_name))
  }
}

rrq_keys_common <- function(queue_id) {
  list(queue_id       = queue_id,

       controller     = sprintf("%s:controller",     queue_id),

       db_prefix      = sprintf("%s:db:",            queue_id),
       envir          = sprintf("%s:envir",          queue_id),

       worker_config  = sprintf("%s:worker:config",  queue_id),
       worker_name    = sprintf("%s:worker:name",    queue_id),
       worker_status  = sprintf("%s:worker:status",  queue_id),
       worker_task    = sprintf("%s:worker:task",    queue_id),
       worker_info    = sprintf("%s:worker:info",    queue_id),
       worker_expect  = sprintf("%s:worker:expect",  queue_id),
       worker_process = sprintf("%s:worker:process", queue_id),

       task_expr      = sprintf("%s:task:expr",      queue_id),
       task_status    = sprintf("%s:task:status",    queue_id),
       task_worker    = sprintf("%s:task:worker",    queue_id),
       task_queue     = sprintf("%s:task:queue",     queue_id),
       task_local     = sprintf("%s:task:local",     queue_id),
       task_progress  = sprintf("%s:task:progress",  queue_id),
       task_result    = sprintf("%s:task:result",    queue_id),
       task_complete  = sprintf("%s:task:complete",  queue_id))
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

rrq_key_queue <- function(queue, name) {
  sprintf("%s:queue:%s", queue, name %||% QUEUE_DEFAULT)
}

## Randomly generated keys:
rrq_key_task_complete <- function(queue) {
  sprintf("%s:tasks:complete:%s", queue, ids::random_id())
}

rrq_key_worker_alive <- function(queue_id) {
  sprintf("%s:worker:alive:%s", queue_id, ids::random_id())
}

rrq_key_queue_deferred <- function(queue, name) {
  sprintf("%s:deferred", rrq_key_queue(queue, name))
}

rrq_key_task_dependencies <- function(queue_id, task_id) {
  sprintf("%s:task:%s:dependencies", queue_id, task_id)
}

rrq_key_task_dependencies_original <- function(queue_id, task_id) {
  sprintf("%s:task:%s:dependencies.original", queue_id, task_id)
}

rrq_key_task_dependents <- function(queue_id, task_id) {
  sprintf("%s:task:%s:dependents", queue_id, task_id)
}
