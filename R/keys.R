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
       configuration  = sprintf("%s:configuration",  queue_id),
       version        = sprintf("%s:version",        queue_id),

       object_store   = sprintf("%s:object_store",   queue_id),
       envir          = sprintf("%s:envir",          queue_id),

       worker_config       = sprintf("%s:worker:config",       queue_id),
       worker_name         = sprintf("%s:worker:name",         queue_id),
       worker_status       = sprintf("%s:worker:status",       queue_id),
       worker_task         = sprintf("%s:worker:task",         queue_id),
       worker_info         = sprintf("%s:worker:info",         queue_id),
       worker_expect       = sprintf("%s:worker:expect",       queue_id),
       worker_process      = sprintf("%s:worker:process",      queue_id),

       task_expr      = sprintf("%s:task:expr",      queue_id),
       task_status    = sprintf("%s:task:status",    queue_id),
       task_worker    = sprintf("%s:task:worker",    queue_id),
       task_queue     = sprintf("%s:task:queue",     queue_id),
       task_local     = sprintf("%s:task:local",     queue_id),
       task_timeout   = sprintf("%s:task:timeout",   queue_id),
       task_progress  = sprintf("%s:task:progress",  queue_id),
       task_result    = sprintf("%s:task:result",    queue_id),
       task_pid       = sprintf("%s:task:pid",       queue_id),

       ## Fault tolerance support
       task_moved_to        = sprintf("%s:task:moved_to",        queue_id),
       task_moved_from      = sprintf("%s:task:moved_from",      queue_id),
       task_moved_root      = sprintf("%s:task:moved_root",      queue_id),

       ## This is the key where we store the extra complete key we
       ## might push to at.
       task_complete  = sprintf("%s:task:complete",  queue_id),
       task_cancel    = sprintf("%s:task:cancel",    queue_id),

       ## Used for tracking times through the task lifecycle
       task_time_submit   = sprintf("%s:task:time_submit",   queue_id),
       task_time_start    = sprintf("%s:task:time_start",    queue_id),
       task_time_complete = sprintf("%s:task:time_complete", queue_id),
       task_time_moved    = sprintf("%s:task:time_moved",    queue_id),

       deferred_set   = sprintf("%s:deferred", queue_id))
}

rrq_keys_worker <- function(queue, worker) {
  list(worker           = worker,
       worker_message   = rrq_key_worker_message(queue, worker),
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

## (Potentially) randomly generated keys:
rrq_key_task_complete <- function(queue, id = NULL) {
  sprintf("%s:tasks:complete:%s", queue, id %||% ids::random_id())
}

rrq_key_worker_alive <- function(queue_id) {
  sprintf("%s:worker:alive:%s", queue_id, ids::random_id())
}

rrq_key_task_dependencies <- function(queue_id, task_id) {
  sprintf("%s:task:%s:dependencies", queue_id, task_id)
}

rrq_key_task_dependencies_original <- function(queue_id, task_id) {
  sprintf("%s:task:%s:dependencies:original", queue_id, task_id)
}

rrq_key_task_dependents <- function(queue_id, task_id) {
  sprintf("%s:task:%s:dependents", queue_id, task_id)
}
