#!/bin/bash

# --- Setup: Script-wide settings and initial directory determination ---

set -euo pipefail

GO_RACE_FLAG="-race"
SHORT_TIMEOUT_CMD="timeout -k 2s 45s"
LONG_TIMEOUT_CMD="timeout -k 2s 120s"

repo_base_dir=$(pwd)
tmp_dir=$(mktemp -d -t mapreduce-tests)

# --- Build Stage: Compile Go binaries ---

declare -a build_targets=(
    "plugins/wordcounter/wordcounter.go"
    "plugins/inverted-index/inverted-index.go"
    "plugins/reduce-delay/reduce-delay.go"
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
sequential_mapreduce="$tmp_dir/sequential_mapreduce"
wordcounter_plugin="$tmp_dir/wordcounter.so"
inverted_index_plugin="$tmp_dir/inverted-index.so"
reduce_delay_plugin="$tmp_dir/reduce-delay.so"
run_master="$tmp_dir/run_master"
run_worker="$tmp_dir/run_worker"

# --- Test Execution Stage: Run tests in temporary directory ---

pushd "$tmp_dir"
failed_any=0

# --- Test: Basic WordCounter Output Check ---
test_name="wordcounter-test"
mkdir "$test_name" && pushd "$test_name"


$sequential_mapreduce $wordcounter_plugin $repo_base_dir/in/pg*.txt
sort mapreduce-out > mapreduce-wc-expected
rm mapreduce-out


$SHORT_TIMEOUT_CMD $run_master $repo_base_dir/in/pg*.txt &
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


$sequential_mapreduce $inverted_index_plugin $repo_base_dir/in/pg*.txt
sort mapreduce-out > mapreduce-index-expected
rm mapreduce-out


$SHORT_TIMEOUT_CMD $run_master $repo_base_dir/in/pg*.txt &
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

DF=anydone$$ # file to check if any worker or master has exited
rm -f $DF

($SHORT_TIMEOUT_CMD $run_master $repo_base_dir/in/pg*.txt; touch $DF) &
master_pid=$!
sleep 1 # allow master to init
for i in {1..3}; do # spawn 3 workers
    ($SHORT_TIMEOUT_CMD $run_worker $reduce_delay_plugin; touch $DF) &
done
jobs

# save the output after any process has exited
while [ ! -e $DF ]; do sleep 0.2; done
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

popd # back to initial directory


# --- Final Exit Status ---
if [ $failed_any -ne 0 ]; then
    echo "--- At least one test failed."
    exit 1
fi
