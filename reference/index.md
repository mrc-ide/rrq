# Package index

## Create queue

- [`rrq_controller()`](https://mrc-ide.github.io/rrq/reference/rrq_controller.md)
  : Create rrq controller
- [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md)
  [`rrq_default_controller_clear()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md)
  : Register default controller

## Tasks

### Creation

- [`rrq_task_create_expr()`](https://mrc-ide.github.io/rrq/reference/rrq_task_create_expr.md)
  : Create a task based on an expression
- [`rrq_task_create_call()`](https://mrc-ide.github.io/rrq/reference/rrq_task_create_call.md)
  : Create a task from a call
- [`rrq_task_create_bulk_expr()`](https://mrc-ide.github.io/rrq/reference/rrq_task_create_bulk_expr.md)
  : Create bulk tasks from an expression
- [`rrq_task_create_bulk_call()`](https://mrc-ide.github.io/rrq/reference/rrq_task_create_bulk_call.md)
  : Create bulk tasks from a call
- [`rrq_task_retry()`](https://mrc-ide.github.io/rrq/reference/rrq_task_retry.md)
  : Retry tasks

### Query

- [`rrq_task_data()`](https://mrc-ide.github.io/rrq/reference/rrq_task_data.md)
  : Fetch internal task data
- [`rrq_task_exists()`](https://mrc-ide.github.io/rrq/reference/rrq_task_exists.md)
  : Test if tasks exist
- [`rrq_task_info()`](https://mrc-ide.github.io/rrq/reference/rrq_task_info.md)
  : Fetch task information
- [`rrq_task_position()`](https://mrc-ide.github.io/rrq/reference/rrq_task_position.md)
  : Find task position in queue
- [`rrq_task_preceeding()`](https://mrc-ide.github.io/rrq/reference/rrq_task_preceeding.md)
  : List tasks ahead of a task
- [`rrq_task_result()`](https://mrc-ide.github.io/rrq/reference/rrq_task_result.md)
  : Fetch single task result
- [`rrq_task_results()`](https://mrc-ide.github.io/rrq/reference/rrq_task_results.md)
  : Get the results of a group of tasks, returning them as a list. See
  rrq_task_result for getting the result of a single task.
- [`rrq_task_status()`](https://mrc-ide.github.io/rrq/reference/rrq_task_status.md)
  : Fetch task statuses
- [`rrq_task_times()`](https://mrc-ide.github.io/rrq/reference/rrq_task_times.md)
  : Fetch task times
- [`rrq_task_wait()`](https://mrc-ide.github.io/rrq/reference/rrq_task_wait.md)
  : Wait for group of tasks
- [`rrq_task_log()`](https://mrc-ide.github.io/rrq/reference/rrq_task_log.md)
  : Fetch task logs

### Overall status

- [`rrq_task_list()`](https://mrc-ide.github.io/rrq/reference/rrq_task_list.md)
  : List all tasks
- [`rrq_task_overview()`](https://mrc-ide.github.io/rrq/reference/rrq_task_overview.md)
  : High level task overview
- [`rrq_deferred_list()`](https://mrc-ide.github.io/rrq/reference/rrq_deferred_list.md)
  : List deferred tasks
- [`rrq_queue_length()`](https://mrc-ide.github.io/rrq/reference/rrq_queue_length.md)
  : Queue length
- [`rrq_queue_list()`](https://mrc-ide.github.io/rrq/reference/rrq_queue_list.md)
  : List queue contents

### Destructive operations

- [`rrq_task_cancel()`](https://mrc-ide.github.io/rrq/reference/rrq_task_cancel.md)
  : Cancel a task
- [`rrq_task_delete()`](https://mrc-ide.github.io/rrq/reference/rrq_task_delete.md)
  : Delete tasks
- [`rrq_queue_remove()`](https://mrc-ide.github.io/rrq/reference/rrq_queue_remove.md)
  : Remove task ids from a queue

## Workers

Workers are the processes that execute tasks. You can create, query and
control workers with these functions

- [`rrq_worker`](https://mrc-ide.github.io/rrq/reference/rrq_worker.md)
  : rrq queue worker
- [`rrq_worker_config()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_config.md)
  : Create worker configuration
- [`rrq_worker_config_list()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_config_list.md)
  : List worker configurations
- [`rrq_worker_config_read()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_config_read.md)
  : Read worker configuration
- [`rrq_worker_config_save()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_config_save.md)
  : Save worker configuration
- [`rrq_worker_delete_exited()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_delete_exited.md)
  : Clean up exited workers
- [`rrq_worker_detect_exited()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_detect_exited.md)
  : Detect exited workers
- [`rrq_worker_envir_set()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_envir_set.md)
  [`rrq_worker_envir_refresh()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_envir_set.md)
  : Set worker environment
- [`rrq_worker_exists()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_exists.md)
  : Test if a worker exists
- [`rrq_worker_info()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_info.md)
  : Worker information
- [`rrq_worker_len()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_len.md)
  : Number of active workers
- [`rrq_worker_list()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_list.md)
  : List active workers
- [`rrq_worker_list_exited()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_list_exited.md)
  : List exited workers
- [`rrq_worker_load()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_load.md)
  : Report on worker load
- [`rrq_worker_log_tail()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_log_tail.md)
  : Returns the last (few) elements in the worker log, in a
  programmatically useful format (see Value).
- [`rrq_worker_process_log()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_process_log.md)
  : Read worker process log
- [`rrq_worker_script()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_script.md)
  : Write worker runner script
- [`rrq_worker_spawn()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_spawn.md)
  : Spawn a worker
- [`rrq_worker_status()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_status.md)
  : Worker statuses
- [`rrq_worker_stop()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_stop.md)
  : Stop workers
- [`rrq_worker_task_id()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_task_id.md)
  : Current task id for workers
- [`rrq_worker_wait()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_wait.md)
  : Wait for workers

## Messages

The messaging interface, see
[`vignette("messages")`](https://mrc-ide.github.io/rrq/articles/messages.md)
for more details

- [`rrq_message_get_response()`](https://mrc-ide.github.io/rrq/reference/rrq_message_get_response.md)
  : Get message response
- [`rrq_message_has_response()`](https://mrc-ide.github.io/rrq/reference/rrq_message_has_response.md)
  : Detect if message has response
- [`rrq_message_response_ids()`](https://mrc-ide.github.io/rrq/reference/rrq_message_response_ids.md)
  : Return ids for messages with responses for a particular worker.
- [`rrq_message_send()`](https://mrc-ide.github.io/rrq/reference/rrq_message_send.md)
  : Send message to workers
- [`rrq_message_send_and_wait()`](https://mrc-ide.github.io/rrq/reference/rrq_message_send_and_wait.md)
  : Send a message and wait for response

## Advanced

- [`rrq_task_progress()`](https://mrc-ide.github.io/rrq/reference/rrq_task_progress.md)
  : Fetch task progress information
- [`rrq_task_progress_update()`](https://mrc-ide.github.io/rrq/reference/rrq_task_progress_update.md)
  : Post task update

### Heartbeat

Interact with the optional heartbeat support

- [`rrq_heartbeat`](https://mrc-ide.github.io/rrq/reference/heartbeat.md)
  : Create a heartbeat instance
- [`rrq_heartbeat_kill()`](https://mrc-ide.github.io/rrq/reference/rrq_heartbeat_kill.md)
  : Kill a process running a heartbeat

## Uncategorised

Things yet to organise nicely

- [`object_store`](https://mrc-ide.github.io/rrq/reference/object_store.md)
  : rrq object store
- [`object_store_offload_disk`](https://mrc-ide.github.io/rrq/reference/object_store_offload_disk.md)
  : Disk-based offload
- [`rrq_destroy()`](https://mrc-ide.github.io/rrq/reference/rrq_destroy.md)
  : Destroy queue
- [`rrq_envir()`](https://mrc-ide.github.io/rrq/reference/rrq_envir.md)
  : Create simple worker environments
