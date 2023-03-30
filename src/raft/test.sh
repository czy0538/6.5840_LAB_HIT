#!/bin/bash

# 执行命令的次数
NUM_OF_RUNS=100

# 命令
COMMAND="go test -run 2A"

# 输出文件名
OUTPUT_FILE="output.log"

# 记录失败数量
FAIL_COUNT=0

for (( i=1; i<=$NUM_OF_RUNS; i++ ))
do
    echo "=== Run #$i ==="
    $COMMAND >> $OUTPUT_FILE 2>&1
    if [ $? -ne 0 ]; then
        FAIL_COUNT=$((FAIL_COUNT+1))
    fi
done

echo "$NUM_OF_RUNS runs completed. $FAIL_COUNT failures found."

if [ $FAIL_COUNT -ne 0 ]; then
    echo "Errors detected in output. Please check $OUTPUT_FILE."
else
    echo "No errors found in output."
fi
