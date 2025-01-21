#!/bin/bash

for i in $(seq 1 25); do wget https://www.gutenberg.org/cache/epub/$i/pg$i.txt -O $(printf "pg-%02d.txt" $i); done
