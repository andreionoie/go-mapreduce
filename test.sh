#!/bin/bash

# --- Setup: Script-wide settings and initial directory determination ---
export PS4=$'\e[1;33m> ${BASH_SOURCE}:${LINENO}: \e[0m'
set -euxo pipefail

GO_RACE_FLAG="-race"
SHORT_TIMEOUT_CMD="timeout -k 2s 45s"
LONG_TIMEOUT_CMD="timeout -k 2s 5m"

repo_base_dir=$(pwd)
tmp_dir=$(mktemp -d -t mapreduce-tests)

# --- Build Stage: Compile Go binaries ---

declare -a build_targets=(
    "plugins/wordcounter/wordcounter.go"
    "plugins/wordcounter-crash-delay/wordcounter-crash-delay.go"
    "plugins/inverted-index/inverted-index.go"
    "plugins/reduce-delay/reduce-delay.go"
    "plugins/jobcount/jobcount.go"
    "plugins/map-timing/map-timing.go"
    "plugins/reduce-timing/reduce-timing.go"

    "cmd/master/run_master.go"
    "cmd/worker/run_worker.go"
    "cmd/sequential/sequential_mapreduce.go"
)

for target in "${build_targets[@]}"; do
    if [[ "$target" == "plugins/"* ]]; then
        plugin="-buildmode=plugin"
    else plugin=""; fi

    go build $GO_RACE_FLAG $plugin -o "$tmp_dir" "$repo_base_dir/$target"
done

# Construct absolute paths to executables
wordcounter_plugin="$tmp_dir/wordcounter.so"
wordcounter_crash_delay_plugin="$tmp_dir/wordcounter-crash-delay.so"
inverted_index_plugin="$tmp_dir/inverted-index.so"
reduce_delay_plugin="$tmp_dir/reduce-delay.so"
jobcount_plugin="$tmp_dir/jobcount.so"
map_timing_plugin="$tmp_dir/map-timing.so"
reduce_timing_plugin="$tmp_dir/reduce-timing.so"

run_master="$tmp_dir/run_master"
run_worker="$tmp_dir/run_worker"
sequential_mapreduce="$tmp_dir/sequential_mapreduce"

# --- Test Execution Stage: Run tests in temporary directory ---

pushd "$tmp_dir"
failed_any=0

# --- Test: Basic WordCounter Output Check ---
test_name="wordcounter-test"
mkdir "$test_name" && pushd "$test_name"

input_files=( $repo_base_dir/in/pg*.txt )
$sequential_mapreduce $wordcounter_plugin ${input_files[@]}
sort mapreduce-out > mapreduce-wc-expected
rm mapreduce-out


$SHORT_TIMEOUT_CMD $run_master ${input_files[@]} &
master_pid=$!

sleep 1 # allow master to init

for i in {1..3}; do # spawn 3 workers
    $SHORT_TIMEOUT_CMD $run_worker $wordcounter_plugin &
done

wait $master_pid
sort mapreduce-out-*-of-* > mapreduce-wc-out

if cmp mapreduce-wc-expected mapreduce-wc-out; then
    echo '---' "$test_name: PASS"
else
    echo '---' "wordcounter output is NOT the same as the sequential version"
    echo '---' "$test_name: FAIL"
    failed_any=1
fi

# wait for all workers processes to finish
wait

popd # back to temp directory

# --- Test: Basic Inverted Index Output Check ---
test_name="inverted-index-test"
mkdir "$test_name" && pushd "$test_name"

input_files=( $repo_base_dir/in/pg*.txt )
$sequential_mapreduce $inverted_index_plugin ${input_files[@]}
sort mapreduce-out > mapreduce-index-expected
rm mapreduce-out


$SHORT_TIMEOUT_CMD $run_master ${input_files[@]} &
master_pid=$!

sleep 1 # allow master to init

for i in {1..3}; do # spawn 3 workers
    $SHORT_TIMEOUT_CMD $run_worker $inverted_index_plugin &
done

wait $master_pid
sort mapreduce-out-*-of-* > mapreduce-index-out

if cmp mapreduce-index-expected mapreduce-index-out; then
    echo '---' "$test_name: PASS"
else
    echo '---' "inverted index output is NOT the same as the sequential version"
    echo '---' "$test_name: FAIL"
    failed_any=1
fi

# wait for all workers processes to finish
wait

popd # back to temp directory

# --- Test: Worker or Master exits before job has completed ---
test_name="earlyexit-test"
mkdir "$test_name" && pushd "$test_name"

input_files=( $repo_base_dir/in/pg*.txt )

(($SHORT_TIMEOUT_CMD $run_master ${input_files[@]}); touch anydone) &
sleep 1 # allow master to init
for i in {1..3}; do # spawn 3 workers
    ($SHORT_TIMEOUT_CMD $run_worker $reduce_delay_plugin; touch anydone) &
done
jobs

# save the output after any process has exited
while [ ! -f anydone ]; do sleep 0.2; done
sort mapreduce-out-*-of-* > mapreduce-out-initial
# save the final output after all exited
wait
sort mapreduce-out-*-of-* > mapreduce-out-final

if cmp mapreduce-out-initial mapreduce-out-final; then
    echo '---' "$test_name: PASS"
else
    echo '---' "output changed after a worker or master exited"
    echo '---' "$test_name: FAIL"
    failed_any=1
fi

popd # back to temp directory

# --- Test: Job Count Test: equals the number of input files ---
test_name="jobcount-test"
mkdir "$test_name" && pushd "$test_name"

input_files=( $repo_base_dir/in/pg*1*.txt )
$SHORT_TIMEOUT_CMD $run_master ${input_files[@]} &
sleep 1

$SHORT_TIMEOUT_CMD $run_worker $jobcount_plugin &
$SHORT_TIMEOUT_CMD $run_worker $jobcount_plugin
$SHORT_TIMEOUT_CMD $run_worker $jobcount_plugin &
$SHORT_TIMEOUT_CMD $run_worker $jobcount_plugin


NT=$(cat mapreduce-out-*-of-* | awk '{print $2}')
if [ "$NT" -eq ${#input_files[@]} ]; then
    echo '---' "$test_name: PASS"
else
    echo '---' "map jobs ran incorrect number of times ($NT != ${#input_files[@]})"
    echo '---' "$test_name: FAIL"
    failed_any=1
fi

wait

popd # back to temp directory

# --- Test: Map Parallelism ---
test_name="map-parallelism-test"
mkdir "$test_name" && pushd "$test_name"

input_files=( $repo_base_dir/in/pg*.txt )
$SHORT_TIMEOUT_CMD $run_master ${input_files[@]} &
sleep 1

$SHORT_TIMEOUT_CMD $run_worker $map_timing_plugin &
$SHORT_TIMEOUT_CMD $run_worker $map_timing_plugin &
$SHORT_TIMEOUT_CMD $run_worker $map_timing_plugin


NT=$(cat mapreduce-out-*-of-* | grep '^times-' | wc -l | sed 's/ //g')
if [ "$NT" != "3" ]
then
  echo '---' "saw $NT workers rather than 3"
  echo '---' "$test_name: FAIL"
  failed_any=1
fi
if cat mapreduce-out-*-of-* | grep '^parallel-[0-9]*.* 3' > /dev/null; then
    echo '---' "$test_name: PASS"
else
    echo '---' "map workers did not run in parallel"
    echo '---' "$test_name: FAIL"
    failed_any=1
fi

wait

popd # back to temp directory

# --- Test: Reduce Parallelism ---
test_name="reduce-parallelism-test"
mkdir "$test_name" && pushd "$test_name"

input_files=( $repo_base_dir/in/pg*.txt )
$SHORT_TIMEOUT_CMD $run_master ${input_files[@]} &
sleep 1

$SHORT_TIMEOUT_CMD $run_worker $reduce_timing_plugin &
$SHORT_TIMEOUT_CMD $run_worker $reduce_timing_plugin


NT=$(cat mapreduce-out-*-of-* | grep '^[a-z] 2' | wc -l | sed 's/ //g')
if [ "$NT" -lt "2" ]
then
  echo '---' "too few parallel reduces"
  echo '---' "$test_name: FAIL"
  failed_any=1
else
    echo '---' "$test_name: PASS"
fi

wait

popd # back to temp directory

# --- Test: Worker Crashing / Time Out ---
test_name="crash-test"
mkdir "$test_name" && pushd "$test_name"

input_files=( $repo_base_dir/in/pg*2*.txt )
$sequential_mapreduce $wordcounter_plugin ${input_files[@]}
sort mapreduce-out > mapreduce-wc-expected
rm mapreduce-out


(($LONG_TIMEOUT_CMD $run_master ${input_files[@]}); touch master-done) &

sleep 1 # allow master to init

for i in {1..3}; do # spawn 3 parallel workers; respawn if master still up
	(while [ -e "/var/tmp/mapreduce-master.sock" -a ! -f master-done ]; do
	    $LONG_TIMEOUT_CMD $run_worker $wordcounter_crash_delay_plugin
		sleep 1
	done) &
done

wait

rm "/var/tmp/mapreduce-master.sock"

sort mapreduce-out-*-of-* > mapreduce-wc-out

if cmp mapreduce-wc-expected mapreduce-wc-out; then
    echo '---' "$test_name: PASS"
else
    echo '---' "wordcounter random crash output is NOT the same as the sequential version"
    echo '---' "$test_name: FAIL"
    failed_any=1
fi

# wait for all workers processes to finish
wait

popd # back to temp directory

popd # back to initial directory


# --- Final Exit Status ---
if [ $failed_any -ne 0 ]; then
    echo "--- At least one test failed."
    exit 1
fi
