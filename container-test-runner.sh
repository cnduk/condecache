#!/usr/bin/env bash

for i in 2.7 3.3 3.4 3.5 3.6
do
    (
        TEMPFILE=$(mktemp)
        docker run --rm -i -v "$(pwd)":/workdir python:$i bash /workdir/_container_run_tests.sh \
            > $TEMPFILE 2>&1 \
            && { rm -f $TEMPFILE; echo "python $i: OK" ; } \
            || { mv $TEMPFILE ./python${i}_test_log; echo "python $i: NOT OK. See  ./python${i}_test_log" ; }
        rm -f $TEMPFILE
    )&
done

wait $(jobs -p)