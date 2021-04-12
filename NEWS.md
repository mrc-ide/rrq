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
