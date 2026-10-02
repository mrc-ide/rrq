# messages

In addition to passing tasks (and results) between a controller and
workers, the controller can also send “messages” to workers. This
vignette shows what the possible messages do.

Generally, you will not need to read this vignette unless you are either
programming against rrq in interesting ways, or you have unusual needs.
Some of the interfaces here may be useful for debugging (particularly
directing particular workers to execute particular pieces of code).

In order to do this, we’re going to need a queue and a worker:

``` r

library(rrq)
id <- paste0("rrq:", ids::random_id(bytes = 4))
obj <- rrq_controller(id)
rrq_default_controller_set(obj)
logdir <- tempfile()
w <- rrq_worker_spawn(logdir = logdir)
#> ℹ Spawning 1 worker with prefix 'nonspheric_siberiantiger'
worker_id <- w$id
```

On startup the worker log contains:

``` plain
[2026-10-02 15:27:23.514136] ALIVE
[2026-10-02 15:27:23.514743] ENVIR new
[2026-10-02 15:27:23.514974] QUEUE default
                                 __
                ______________ _/ /
      ______   / ___/ ___/ __ `/ /_____
     /_____/  / /  / /  / /_/ /_/_____/
 ______      /_/  /_/   \__, (_)   ______
/_____/                   /_/     /_____/
    worker:        nonspheric_siberiantiger_1
    config:        localhost
    rrq_version:   0.7.24
    platform:      x86_64-pc-linux-gnu
    running:       Ubuntu 24.04.5 LTS
    hostname:      runnervm8df0l
    username:      runner
    queue:         rrq:75a1b6ca:queue:default
    wd:            /home/runner/work/rrq/rrq/vignettes
    pid:           7980
    redis_host:    127.0.0.1
    redis_port:    6379
    heartbeat_key: <not set>
    offload_path:  <not set>
```

Because one of the main effects of messages is to print to the worker
logfile, we’ll print this fairly often.

## Messages and responses

1.  The queue sends a message for one or more workers to process. The
    message has an *identifier* that is derived from the current time.
    Messages are written to a first-in-first-out queue, *per worker*,
    and are processed independently by workers who do not look to see if
    other workers have messages or are processing them.

2.  As soon as a worker has finished processing any current job it will
    process the message (it must wait to finish a current job but will
    not start any further jobs).

3.  Once the message has been processed (see below) a response will be
    written to a response list with the same identifier as the message.

Some messages interact with the worker timeout:

- `PING`, `ECHO`, `EVAL`, `INFO` `PAUSE`, `RESUME` and `REFRESH` will
  reset the timer, as if a task had been run
- `TIMEOUT_SET` explicitly interacts with the timer
- `TIMEOUT_GET` does not reset the timer, reporting the remaining time
- `STOP` causes the worker to exit, so has no interaction with the timer

## `PING`

The `PING` message simply asks the worker to return `PONG`. It’s useful
for diagnosing communication issues because it does so little

``` r

message_id <- rrq_message_send("PING")
```

The message id is going to be useful for getting responses:

``` r

message_id
#> [1] "1790954843.587266"
```

(this is derived from the current time, according to Redis which is the
central reference point of time for the whole system).

``` plain
[2026-10-02 15:27:23.587517] MESSAGE PING
PONG
[2026-10-02 15:27:23.587915] RESPONSE PING
```

The logfile prints:

1.  the request for the `PING` (`MESSAGE PING`)
2.  the value `PONG` to the R message stream
3.  logging a response (`RESPONSE PONG`), which means that something is
    written to the response stream.

We can access the same bits of information in the worker log:

``` r

rrq_worker_log_tail(n = Inf)
#>                    worker_id child       time  command message
#> 1 nonspheric_siberiantiger_1    NA 1790954844    ALIVE        
#> 2 nonspheric_siberiantiger_1    NA 1790954844    ENVIR     new
#> 3 nonspheric_siberiantiger_1    NA 1790954844    QUEUE default
#> 4 nonspheric_siberiantiger_1    NA 1790954844  MESSAGE    PING
#> 5 nonspheric_siberiantiger_1    NA 1790954844 RESPONSE    PING
```

This includes the `ALIVE` message as the worker comes up.

Inspecting the logs is fine for interactive use, but it’s going to be
more useful often to poll for a response.

We already know that our worker has a response, but we can ask anyway:

``` r

rrq_message_has_response(message_id)
#> nonspheric_siberiantiger_1 
#>                       TRUE
```

Or inversely we can as what messages a given worker has responses for:

``` r

rrq_message_response_ids(worker_id)
#> [1] "1790954843.587266"
```

To fetch the responses from all workers it was sent to (always returning
a named list):

``` r

rrq_message_get_response(message_id)
#> $nonspheric_siberiantiger_1
#> [1] "PONG"
```

or to fetch the response from a given worker:

``` r

rrq_message_get_response(message_id, worker_id)
#> $nonspheric_siberiantiger_1
#> [1] "PONG"
```

The response can be deleted by passing `delete = TRUE` to this method:

``` r

rrq_message_get_response(message_id, worker_id, delete = TRUE)
#> $nonspheric_siberiantiger_1
#> [1] "PONG"
```

after which recalling the message will throw an error:

``` r

rrq_message_get_response(message_id, worker_id)
#> Error in `rrq_message_get_response()`:
#> ! Response missing for worker: 'nonspheric_siberiantiger_1'
```

There is also a `timeout` argument that lets you wait until a response
is ready (as in
[`rrq_task_wait()`](https://mrc-ide.github.io/rrq/reference/rrq_task_wait.md)).

``` r

rrq_task_create_expr(Sys.sleep(2))
#> [1] "3f5e3b98ec33ba29a58c159bc165ae1f"
message_id <- rrq_message_send("PING")
rrq_message_get_response(
  message_id, worker_id, delete = TRUE, timeout = 10)
#> $nonspheric_siberiantiger_1
#> [1] "PONG"
```

Looking at the log will show what went on here:

``` r

rrq_worker_log_tail(n = 4)
#>                    worker_id child       time       command
#> 1 nonspheric_siberiantiger_1    NA 1790954844    TASK_START
#> 2 nonspheric_siberiantiger_1    NA 1790954846 TASK_COMPLETE
#> 3 nonspheric_siberiantiger_1    NA 1790954846       MESSAGE
#> 4 nonspheric_siberiantiger_1    NA 1790954846      RESPONSE
#>                            message
#> 1 3f5e3b98ec33ba29a58c159bc165ae1f
#> 2 3f5e3b98ec33ba29a58c159bc165ae1f
#> 3                             PING
#> 4                             PING
```

1.  A task is received
2.  2s later the task is completed
3.  Then the message is received
4.  Then, basically instantaneously, the message is responded to

However, because the message is only processed after the task is
completed, the response takes a while to come back. Equivalently, from
the worker log:

``` plain
[2026-10-02 15:27:24.034302] TASK_START 3f5e3b98ec33ba29a58c159bc165ae1f
[2026-10-02 15:27:26.039995] TASK_COMPLETE 3f5e3b98ec33ba29a58c159bc165ae1f
[2026-10-02 15:27:26.040996] MESSAGE PING
PONG
[2026-10-02 15:27:26.041178] RESPONSE PING
```

## `ECHO`

This is basically like `PING` and not very interesting; it prints an
arbitrary string to the log. It always returns `"OK"` as a response.

``` r

message_id <- rrq_message_send("ECHO", "hello world!")
rrq_message_get_response(message_id, worker_id, timeout = 10)
#> $nonspheric_siberiantiger_1
#> [1] "OK"
```

``` plain
[2026-10-02 15:27:26.672121] MESSAGE ECHO
hello world!
[2026-10-02 15:27:26.672913] RESPONSE ECHO
```

## `INFO`

The `INFO` command refreshes and returns the worker information.

We already have a copy of the worker info; it was created when the
worker started up:

``` r

rrq_worker_info()[[worker_id]]
#>   <rrq_worker_info>
#>     worker:        nonspheric_siberiantiger_1
#>     config:        localhost
#>     rrq_version:   0.7.24
#>     platform:      x86_64-pc-linux-gnu
#>     running:       Ubuntu 24.04.5 LTS
#>     hostname:      runnervm8df0l
#>     username:      runner
#>     queue:         rrq:75a1b6ca:queue:default
#>     wd:            /home/runner/work/rrq/rrq/vignettes
#>     pid:           7980
#>     redis_host:    127.0.0.1
#>     redis_port:    6379
#>     heartbeat_key: NULL
#>     offload_path:  NULL
```

We can force the worker to refresh:

``` r

message_id <- rrq_message_send("INFO")
```

Here’s the new worker information, complete with an updated `envir`
field:

``` r

rrq_message_get_response(message_id, worker_id, timeout = 10)
#> $nonspheric_siberiantiger_1
#> $nonspheric_siberiantiger_1$worker
#> [1] "nonspheric_siberiantiger_1"
#> 
#> $nonspheric_siberiantiger_1$config
#> [1] "localhost"
#> 
#> $nonspheric_siberiantiger_1$rrq_version
#> [1] "0.7.24"
#> 
#> $nonspheric_siberiantiger_1$platform
#> [1] "x86_64-pc-linux-gnu"
#> 
#> $nonspheric_siberiantiger_1$running
#> [1] "Ubuntu 24.04.5 LTS"
#> 
#> $nonspheric_siberiantiger_1$hostname
#> [1] "runnervm8df0l"
#> 
#> $nonspheric_siberiantiger_1$username
#> [1] "runner"
#> 
#> $nonspheric_siberiantiger_1$queue
#> [1] "rrq:75a1b6ca:queue:default"
#> 
#> $nonspheric_siberiantiger_1$wd
#> [1] "/home/runner/work/rrq/rrq/vignettes"
#> 
#> $nonspheric_siberiantiger_1$pid
#> [1] 7980
#> 
#> $nonspheric_siberiantiger_1$redis_host
#> [1] "127.0.0.1"
#> 
#> $nonspheric_siberiantiger_1$redis_port
#> [1] 6379
#> 
#> $nonspheric_siberiantiger_1$heartbeat_key
#> NULL
#> 
#> $nonspheric_siberiantiger_1$offload_path
#> NULL
```

## `EVAL`

Evaluate an arbitrary R expression, passed as a string (*not* as any
sort of unevaluated or quoted expression). This expression is evaluated
in the global environment, which is *not* the environment in which
queued code is evaluated in.

``` r

message_id <- rrq_message_send("EVAL", "1 + 1")
rrq_message_get_response(message_id, worker_id, timeout = 10)
#> $nonspheric_siberiantiger_1
#> [1] 2
```

This could be used to evaluate code that has side effects, such as
installing packages. However, due to limitations with how R loads
packages the only way to update and reload a package is going to be to
restart the worker.

## `PAUSE` / `RESUME`

The `PAUSE` / `RESUME` messages can be used to prevent workers from
picking up new work (and then allowing them to start again).

``` r

rrq_worker_status()
#> nonspheric_siberiantiger_1 
#>                     "IDLE"
message_id <- rrq_message_send("PAUSE")
rrq_message_get_response(message_id, worker_id, timeout = 10)
#> $nonspheric_siberiantiger_1
#> [1] "OK"
rrq_worker_status()
#> nonspheric_siberiantiger_1 
#>                   "PAUSED"
```

Once paused workers ignore tasks, which stay on the queue:

``` r

t <- rrq_task_create_expr(runif(5))
rrq_task_status(t)
#> [1] "PENDING"
```

Sending a `RESUME` message unpauses the worker:

``` r

message_id <- rrq_message_send("RESUME")
rrq_message_get_response(message_id, worker_id, timeout = 10)
#> $nonspheric_siberiantiger_1
#> [1] "OK"
rrq_task_wait(t, 5)
#> [1] TRUE
```

## `SET_TIMEOUT` / `GET_TIMEOUT`

Workers will quit after being left idle for more than a certain time;
this is their timeout. Only processing tasks counts as work (not
messages). You can query the timeout with `GET_TIMEOUT` and set it with
`SET_TIMEOUT`. For our worker above the timeout is infinite; it will
never quit:

``` r

rrq_message_send_and_wait("TIMEOUT_GET", worker_ids = worker_id)
#> $nonspheric_siberiantiger_1
#> timeout_idle    remaining 
#>          Inf          Inf
```

We can set this to a finite value, in seconds:

``` r

rrq_message_send_and_wait("TIMEOUT_SET", 600, worker_ids = worker_id)
#> $nonspheric_siberiantiger_1
#> [1] "OK"
```

Here the timeout is set to 10 minutes (600s).

Once set, the `TIMEOUT_GET` returns the length of time remaining before
the worker exits

``` r

rrq_message_send_and_wait("TIMEOUT_GET", worker_ids = worker_id)
#> $nonspheric_siberiantiger_1
#> timeout_idle    remaining 
#>     600.0000     599.9088
Sys.sleep(5)
rrq_message_send_and_wait("TIMEOUT_GET", worker_ids = worker_id)
#> $nonspheric_siberiantiger_1
#> timeout_idle    remaining 
#>     600.0000     594.8513
```

One useful pattern is to send work to workers, then set the timeout to
zero. This means that when work is complete they will exit (almost)
immediately:

``` r

ids <- rrq_task_create_bulk_call(function(x) {
  Sys.sleep(0.5)
  runif(x)
}, 1:5)
rrq_message_send("TIMEOUT_SET", 0, worker_id)
rrq_task_wait(ids)
#> [1] TRUE
rrq_task_results(ids)
#> [[1]]
#> [1] 0.9956762
#> 
#> [[2]]
#> [1] 0.3466077 0.7420148
#> 
#> [[3]]
#> [1] 0.6893818 0.2001051 0.1371164
#> 
#> [[4]]
#> [1] 0.6351213 0.2176908 0.2525733 0.2447310
#> 
#> [[5]]
#> [1] 0.0002912206 0.3981710155 0.0630658732 0.3631322302 0.4149988941
```

The worker will remain idle for 60s (by default) which is the length of
time that one poll for work lasts, then it will exit.

``` r

rrq_worker_status(worker_id)
#> nonspheric_siberiantiger_1 
#>                     "IDLE"
rrq_message_send_and_wait("TIMEOUT_GET", worker_ids = worker_id)
#> $nonspheric_siberiantiger_1
#> timeout_idle    remaining 
#>            0            0
```

## Messages that are supported but use via wrappers:

There are other methods that are typically used via methods on the
\[`rrq_controller`\] object.

- `REFRESH`: requests that the worker refresh its evaluation
  environment. Typically used via
  [`rrq_worker_envir_set()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_envir_set.md)
- `STOP`: sent with a informational message as an argument, requests
  that the worker stop. Typically used via
  [`rrq_worker_stop()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_stop.md)
