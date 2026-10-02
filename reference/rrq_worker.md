# rrq queue worker

rrq queue worker

rrq queue worker

## Details

A rrq queue worker. These are not typically for interacting with but
will sit and poll a queue for jobs.

## Public fields

- `id`:

  The id of the worker

- `config`:

  The name of the configuration used by this worker

- `controller`:

  An rrq controller object

## Methods

### Public methods

- [`rrq_worker$new()`](#method-rrq_worker-new)

- [`rrq_worker$info()`](#method-rrq_worker-info)

- [`rrq_worker$log()`](#method-rrq_worker-log)

- [`rrq_worker$load_envir()`](#method-rrq_worker-load_envir)

- [`rrq_worker$poll()`](#method-rrq_worker-poll)

- [`rrq_worker$step()`](#method-rrq_worker-step)

- [`rrq_worker$loop()`](#method-rrq_worker-loop)

- [`rrq_worker$format()`](#method-rrq_worker-format)

- [`rrq_worker$timer_start()`](#method-rrq_worker-timer_start)

- [`rrq_worker$progress()`](#method-rrq_worker-progress)

- [`rrq_worker$task_eval()`](#method-rrq_worker-task_eval)

- [`rrq_worker$shutdown()`](#method-rrq_worker-shutdown)

------------------------------------------------------------------------

### Method `new()`

Constructor

#### Usage

    rrq_worker$new(
      queue_id,
      name_config = "localhost",
      worker_id = NULL,
      timeout_config = 0,
      is_child = FALSE,
      con = redux::hiredis(),
      offload_path = NULL
    )

#### Arguments

- `queue_id`:

  The queue id

- `name_config`:

  Optional name of the configuration. The default "localhost"
  configuration always exists. Create new configurations using
  [rrq_worker_config_save](https://mrc-ide.github.io/rrq/reference/rrq_worker_config_save.md).

- `worker_id`:

  Optional worker id. If omitted, a random id will be created.

- `timeout_config`:

  How long to try and read the worker configuration for. Will attempt to
  read once a second and throw an error if config cannot be located
  after `timeout` seconds. Use this to create workers before their
  configurations are available. The default (0) is to assume that the
  configuration is immediately available.

- `is_child`:

  Logical, used to indicate that this is a child of the real worker. If
  `is_child` is `TRUE`, then most other arguments here have no effect
  (e.g., `queue` all the timeout / idle / polling arguments) as they
  come from the parent. Not for general use.

- `con`:

  A redis connection

- `offload_path`:

  The path to create an offload store at. See
  [rrq_controller](https://mrc-ide.github.io/rrq/reference/rrq_controller.md)
  for details.

------------------------------------------------------------------------

### Method `info()`

Return information about this worker, a list of key-value pairs.

#### Usage

    rrq_worker$info()

------------------------------------------------------------------------

### Method [`log()`](https://rdrr.io/r/base/Log.html)

Create a log entry. This will print a human readable format to screen
and a machine-readable format to the redis database.

#### Usage

    rrq_worker$log(label, value = NULL)

#### Arguments

- `label`:

  Scalar character, the title of the log entry

- `value`:

  Character vector (or null) with log values

------------------------------------------------------------------------

### Method `load_envir()`

Load the worker environment by creating a new environment object and
running the create hook (if configured). See
[`rrq_worker_envir_set()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_envir_set.md)
for details.

#### Usage

    rrq_worker$load_envir()

------------------------------------------------------------------------

### Method `poll()`

Poll for work

#### Usage

    rrq_worker$poll(immediate = FALSE)

#### Arguments

- `immediate`:

  Logical, indicating if we should *not* do a blocking wait on the queue
  but instead reducing the timeout to zero. Intended primarily for use
  in the tests.

------------------------------------------------------------------------

### Method [`step()`](https://rdrr.io/r/stats/step.html)

Take a single "step". This consists of

1.  Poll for work (`$poll()`)

2.  If work found, run it (either a task or a message)

3.  If work not found, check the timeout

#### Usage

    rrq_worker$step(immediate = FALSE)

#### Arguments

- `immediate`:

  Logical, indicating if we should *not* do a blocking wait on the queue
  but instead reducing the timeout to zero. Intended primarily for use
  in the tests.

------------------------------------------------------------------------

### Method `loop()`

The main worker loop. Use this to set up the main worker event loop,
which will continue until exiting (via a timeout or message).

#### Usage

    rrq_worker$loop(immediate = FALSE)

#### Arguments

- `immediate`:

  Logical, indicating if we should *not* do a blocking wait on the queue
  but instead reducing the timeout to zero. Intended primarily for use
  in the tests.

------------------------------------------------------------------------

### Method [`format()`](https://rdrr.io/r/base/format.html)

Create a nice string representation of the worker. Used automatically to
print the worker by R6.

#### Usage

    rrq_worker$format()

------------------------------------------------------------------------

### Method `timer_start()`

Start the timer

#### Usage

    rrq_worker$timer_start()

------------------------------------------------------------------------

### Method `progress()`

Submit a progress message. See
[`rrq_task_progress_update()`](https://mrc-ide.github.io/rrq/reference/rrq_task_progress_update.md)
for details of this mechanism.

#### Usage

    rrq_worker$progress(value, error = TRUE)

#### Arguments

- `value`:

  An R object with the contents of the update. This will overwrite any
  previous progress value, and can be retrieved with
  [rrq_task_progress](https://mrc-ide.github.io/rrq/reference/rrq_task_progress.md).
  A value of `NULL` will appear to clear the status, as `NULL` will also
  be returned if no status is found for a task.

- `error`:

  Logical, indicating if we should throw an error if not running as an
  `rrq` task. Set this to `FALSE` if you want code to work without
  modification within and outside of an `rrq` job, or to `TRUE` if you
  want to be sure that progress messages have made it to the server.

------------------------------------------------------------------------

### Method `task_eval()`

Evaluate a task. When running a task on a separate process, we will
always set two environment variables: \* `RRQ_WORKER_ID` this is the id
field \* `RRQ_TASK_ID` this is the task id

#### Usage

    rrq_worker$task_eval(task_id)

#### Arguments

- `task_id`:

  A task identifier. It is undefined what happens if this identifier
  does not exist.

------------------------------------------------------------------------

### Method `shutdown()`

Stop the worker

#### Usage

    rrq_worker$shutdown(status = "OK", graceful = TRUE)

#### Arguments

- `status`:

  the worker status; typically be one of `OK` or `ERROR` but can be any
  string

- `graceful`:

  Logical, indicating if we should request a graceful shutdown of the
  heartbeat, if running.
