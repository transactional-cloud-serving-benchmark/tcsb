#!/usr/bin/env sh
set -xe
mkdir -p artifacts

for f in $(ls ./cmd); do
	go build -o artifacts/${f} ./cmd/${f}
done
