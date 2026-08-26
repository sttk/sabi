#!/usr/bin/env bash

set -euo pipefail

clean() {
  go clean --cache
}

format() {
  go fmt ./...
}

compile() {
  go vet ./...
  go build ./...
}

test() {
  go test -v $(go list ./... | grep -v /benchmark)
}

unit() {
  go test -v -run $1 $(go list ./... | grep -v /benchmark)
}

cover() {
  mkdir -p coverage
  go test -coverprofile=coverage/cover.out $(go list ./... | grep -v /benchmark)
  go tool cover -html=coverage/cover.out -o coverage/cover.html
}

bench() {
  local dir=$1
  if [[ "$dir" == "" ]]; then
    dir="."
  fi
  pushd $dir
  go test -bench . --benchmem
  popd
}

if [[ "$#" == "0" ]]; then
  clean
  format
  compile
  test
  cover

elif [[ "$1" == "unit" ]]; then
  unit $2

elif [[ "$1" == "bench" ]]; then
  bench $2

else
  for a in "$@"; do
    case "$a" in
    clean)
      clean
      ;;
    format)
      format
      ;;
    compile)
      compile
      ;;
    test)
      test
      ;;
    cover)
      cover
      ;;
    '')
      compile
      ;;
    *)
      echo "Bad task: $a"
      exit 1
      ;;
    esac
  done
fi
