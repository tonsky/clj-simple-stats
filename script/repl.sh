#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "`dirname $0`/.."

clojure $(./script/java_opts.sh) -X:dev clojure+.core.server/start-server