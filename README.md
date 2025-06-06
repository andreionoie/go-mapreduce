# distributed MapReduce framework in Go


```mermaid
%%{init: {
  'theme': 'base',
  'themeVariables': {
    'fontFamily': 'monospace',
    'fontSize': '10px',
    'primaryColor': '#f8f9fa',
    'edgeLabelBackground': '#ffffff',
    'lineColor': '#6c757d'
  }
}}%%
flowchart LR

    %% ------------------------------------------------
    %% CLASS DEFINITIONS (colors, borders, text)
    %% ------------------------------------------------
    classDef input        fill:#CCE5FF,    stroke:#004085, stroke-width:1px, color:#004085;
    classDef map          fill:#D4EDDA,    stroke:#155724, stroke-width:1px, color:#155724;
    classDef intermediate fill:#FFF3CD,    stroke:#856404, stroke-width:1px, color:#856404;
    classDef reduce       fill:#F8D7DA,    stroke:#721C24, stroke-width:1px, color:#721C24;
    classDef output       fill:#D1ECF1,    stroke:#0C5460, stroke-width:1px, color:#0C5460;
    classDef header       fill:none,       stroke:none,     color:#212529, font-weight:bold;

    %% ------------------------------------------------
    %% 1) INPUT FILES
    %% ------------------------------------------------
    subgraph Inputs
        direction TB
        i0(["pg0.txt"]):::input
        iDots(["..."]):::input
        iM_1(["pg[M-1].txt"]):::input
    end

    %% ------------------------------------------------
    %% 2) MAP TASKS
    %% ------------------------------------------------
    subgraph Map Phase
        direction TB
        M0([Map Task 0]):::map
        Mdots(["..."]):::map
        Mlast(["Map Task [M-1]"]):::map
    end

    %% Edges from inputs to corresponding map tasks
    i0     --> M0
    iDots  --> Mdots
    iM_1   --> Mlast

    %% ------------------------------------------------
    %% 3) INTERMEDIATE FILES
    %% ------------------------------------------------
    subgraph Intermediate Files
        direction TB

        subgraph "Partition R = 0"
            direction TB
            IR00(["intermediate-M0-R0.json"]):::intermediate
            IRdots0(["..."]):::intermediate
            IRM_1_0(["intermediate-M[M-1]-R0.json"]):::intermediate
        end

        subgraph "..."
            direction TB
            middleDots(["..."]):::intermediate
        end

        subgraph "Partition R = R-1"
            direction TB
            IR0R(["intermediate-M0-R[R-1].json"]):::intermediate
            IRdotsR(["..."]):::intermediate
            IRM_1_R(["intermediate-M[M-1]-R[R-1].json"]):::intermediate
        end
    end

    %% Map task outputs -> intermediate files
    M0     --> IR00
    M0     --> IR0R
    M0     --> middleDots

    Mdots  --> IRdots0
    Mdots  --> IRdotsR
    Mdots  --> middleDots

    Mlast  --> IRM_1_0
    Mlast  --> IRM_1_R
    Mlast  --> middleDots

    %% ------------------------------------------------
    %% 4) REDUCE TASKS
    %% ------------------------------------------------
    subgraph Reduce Phase
        direction TB
        R0([Reduce Task 0]):::reduce
        Rdots(["..."]):::reduce
        Rlast(["Reduce Task [R-1]"]):::reduce
    end

    %% Intermediate files -> corresponding reduce tasks
    IR00        --> R0
    IRdots0     --> R0
    IRM_1_0     --> R0

    middleDots  --> Rdots

    IR0R        --> Rlast
    IRdotsR     --> Rlast
    IRM_1_R     --> Rlast

    %% ------------------------------------------------
    %% 5) OUTPUTS
    %% ------------------------------------------------
    subgraph Outputs
        direction TB
        out0(["mapreduce-out-0-of-[R-1]"]):::output
        outDots(["..."]):::output
        outLast(["mapreduce-out-[R-1]-of-[R-1]"]):::output
    end

    %% Final outputs after reduce
    R0      --> out0
    Rdots   --> outDots
    Rlast   --> outLast
```

a lightweight implementation of MapReduce (https://research.google.com/archive/mapreduce-osdi04.pdf) with a master / worker architecture written entirely in Go’s standard library.
workers may join and leave at any time; the master detects failures, re-assigns unfinished tasks, and guarantees that each map or reduce job is applied at most once.


## components

### [master coordinator (`cmd/master/`)](./cmd/master/)
-	creates a Map task for every input file and a configurable number of Reduce partitions (`NReduce`).
-	exposes two RPC methods over a Unix-domain socket (`/var/tmp/mapreduce-master.sock`):
    - WorkerTaskRequest: hands out new work
    - WorkerTaskUpdate: records progress/completion.
-	tracks every task’s State (Idle | InProgress | Completed | Error) and reverts a task to Idle if the assigned worker is silent for 10s.
-	when all maps finish, automatically groups the emitted intermediate JSON files into reduce buckets and spins up Reduce tasks.
-	shuts down cleanly after the last reduce completes

### [worker process (`cmd/worker/`)](./cmd/worker/)
-	dynamically loads any Go plugin (.so) that exports:
```go
    func Map(filename, contents string) []mapreduce.KVPair
    func Reduce(key string, values []string) string
```
(see [plugins/](./plugins/) for examples).

-   idle workers continously poll the master coordinator for new MapTasks or ReduceTasks to execute
-	map phase: reads its input split `M`, calls Map, hashes keys to obtain the reduce bucket number `r`, and writes `intermediate-M-r.json` for each.
-	reduce phase: reads each `[intermediate-1-r.json, intermediate-2-r.json, .., intermediate-M-r.json]` for a specific `r` bucket, sorts by key & groups together, calls Reduce, and writes `mapreduce-out-r-of-R`.
-	retries: if a task times out or the worker crashes, another worker re-executes it; duplicate execution is harmless by design.

### [core library (`mapreduce/`)](./mapreduce/)
-	RPC types & helpers – TaskType, TaskState, TaskResponseType, CallMaster
-	task scheduler – logic for timeouts, re-assignment, and reduce-task synthesis lives in [`master.go`](./mapreduce/master.go)
-	worker runtime – [`worker.go`](./mapreduce/worker.go) contains the event loop, map/reduce helpers, and a readable worker name generator (e.g., GroovyLynx_4242).

### [example plugins (`plugins/`)](./plugins/)
| name                            | description                                  | path                                                               |
|---------------------------------|----------------------------------------------|--------------------------------------------------------------------|
| `wordcounter`                   | canonical “word count”                       | [plugins/wordcounter/wordcounter.go](./plugins/wordcounter/wordcounter.go) |
| `inverted-index`                | emits word -> [file, positions]              | [plugins/inverted-index/inverted-index.go](./plugins/inverted-index/inverted-index.go) |
| `map-timing` / `reduce-timing`  | verify parallelism                           | [plugins/map-timing/map-timing.go](./plugins/map-timing/map-timing.go) and [plugins/reduce-timing/reduce-timing.go](./plugins/reduce-timing/reduce-timing.go) |
| `jobcount`                      | ensure each map runs once                    | [plugins/jobcount/jobcount.go](./plugins/jobcount/jobcount.go) |
| `reduce-delay`                  | stress long‐running reduce                   | [plugins/reduce-delay/reduce-delay.go](./plugins/reduce-delay/reduce-delay.go) |
| `wordcounter-crash-delay`       | random crashes & delays to test recovery     | [plugins/wordcounter-crash-delay/wordcounter-crash-delay.go](./plugins/wordcounter-crash-delay/wordcounter-crash-delay.go) |

### minimal sequential runner ([cmd/sequential/](./cmd/sequential/))
a single-process baseline used only to verify the correctness of the distributed version's output.

------

## technical highlights
- fault-tolerant scheduling – master marks a task Expired after 10 s of silence and hands it to another worker.
- at-most-once semantics – duplicates are possible but harmless; tests confirm final output equals sequential ground truth.
- parallel map & reduce – any idle worker can execute any pending task, enabling full utilisation of machines across clusters.
- plugin architecture – swap in a new .so without recompiling the framework.
- temporary‐file pattern - every intermediate and final output is first written to a randomly‐named temp file (via os.CreateTemp), then atomically renamed to its final, deterministic filename
    - this prevents partial writes if a worker crashes mid‐write
- comprehensive test harness – test.sh runs six end-to-end scenarios: crash/retry, early exit, timing, etc.



