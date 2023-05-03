# rrq 0.7.0

* Overhaul worker configuration and creation; now all worker configuration options (except redis and storage control) are set through a named worker configuration; this reduces the number of arguments being passed around and improves documentation (mrc-4068)

# rrq 0.6.16

* Extend fault tolerance support by enabling tasks to be retried; this creates a new task that the old task points at and can be used to seamlessly recover from all sorts of task failure (mrc-2683)
* Queues print vertically when printing out worker info (mrc-4114)

# rrq 0.6.14

* New `$task_info` method to retrieve more detailed information about where a task was run (mrc-4058)

# rrq 0.6.13

* Argument standardisation for timeouts, with changes:
  * `collect_timeout` in bulk functions becomes `timeout_task_wait`
  * `worker_stop_timeout` in the `rrq_controller`'s `destroy` method becomes `timeout_worker_stop`
  * `task_timeout` in queuing a task on a separate process, in bulk submission functions becomes `timeout_task_run`
  * `timeout` in `enqueue` becomes `timeout_task_run`

# rrq 0.6.12

* New argument `timeout_task_wait` and option `rrq.timeout_task_wait` to control the default time to wait for tasks to be returned from `task_wait` and bulk task retrieval methods. The default behaviour is unchanged (blocking indefinitely) but this can now easily be changed at a global or queue-scoped level.

# rrq 0.6.11

* Changed argument of `worker_config_save` from `timeout` to `timeout_idle` (mrc-4075)

# rrq 0.6.10

* It is now possible to (optionally) throw task errors on retrieval via `task_wait`, `task_results` and the bulk interfaces. The default remains not to error.
* Bulk submitted tasks no longer auto-delete.

# rrq 0.6.7

* Nicer errors that tell you what the contain, particularly when they have stack traces or warnings (mrc-1260)

# rrq 0.6.6

* Drop `at_front` argument (introduced in rrq 0.2.14), tasks can no longer jump the queue (mrc-4069)

# rrq 0.6.5

* Error traces now come from `rlang` and are much nicer to read (mrc-4060)

# rrq 0.6.4

* Make separate process timeouts `timeout_poll` and `timeout_die` configurable via the worker config.

# rrq 0.6.1

* Simplify version information returned by `worker_info` (mrc-2295)

# rrq 0.6.0

* Renamed some functions with `rrq_` prefix; you must now use `rrq_worker_spawn()` (not `worker_spawn`), `rrq_worker_wait`, `rrq_heartbeat` and `rrq_heartbeat_kill` (mrc-2682)

# rrq 0.4.1

* Expose `rrq::rrq_worker_from_config`, which is a simpler way of constructing worker objects. The worker object itself (`rrq::rrq_worker`) loses its helper constructor function and is documented (mrc-2297)

# rrq 0.3.2

* All tasks get a key that can be checked with a blocking wait (mrc-2392)

# rrq 0.3.1

* Direct control over environment export for `$enqueue()` with new argument `export` (mrc-2369)

# rrq 0.2.19

* Add new `enqueue_bulk` method (previously deleted in 0.2.0) (mrc-2261)

# rrq 0.2.15

* `enqueue` can add a task with dependencies i.e. tasks which must be complete before this task can be run via `depends_on` (mrc-2255)

# rrq 0.2.14

* `enqueue` can add a task to start of the queue via `at_front = TRUE` (mrc-2254)

# rrq 0.2.13

* Run tasks in a separate process (with some overhead) with new argument `separate_process = TRUE` to `$enqueue()`. Use this to ensure isolation between tasks (mrc-2068)

# rrq 0.2.12

* Add `task_preceeding` function to controller to list tasks in front of a particular task in the queue (vimc-4502)

# rrq 0.2.11

* Support for multiple queues, with varying priorities. This can be used to create workers that listen to overlapping queues, with "fast" and "slow" queues (mrc-2068)

# rrq 0.2.10

* Gracefully detect multiple killed workers (#22, reported by @MartinHanewald)

# rrq 0.2.9

* `$lapply` and friends restored after being removed during the refactor for version 0.2.0 (mrc-558)

# rrq 0.2.8

* Expand documentation (mrc-1800)

# rrq 0.2.7

* rrq progress now passes all fields in underlying condition (mrc-1772)

# rrq 0.2.6

* Update `worker_spawn` to work with breaking change in docopt (mrc-1667)

# rrq 0.2.5

* New `$task_data` method for getting underlying task data (mrc-1304)

# rrq 0.2.4

* Better error message is given when non-existent task is cancelled (mrc-1259)

# rrq 0.2.3

* New `$worker_detect_exited` for detecting exited workers when a heartbeat is used (mrc-1231)

# rrq 0.2.2

* Tasks can now be interrupted with `$task_cancel` if running with a heartbeat enabled (mrc-734)

# rrq 0.2.1

* Add support for within-task progress updates, using the `rrq::rrq_task_progress_update` function, which can be called from any task run from `rrq` and queried with `$task_progress` from a `rrq_controller` (mrc-600)

# rrq 0.2.0

* Rewrite of the package to simplify queue creation and dependency chain (mrc-538 / #9, mrc-519 / #8, mrc-472 / #7)
