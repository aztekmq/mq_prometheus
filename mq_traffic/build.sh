#!/usr/bin/env bash
set -euo pipefail
dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
mq=$(cd -- "${MQ_INSTALLATION_PATH:-"$dir/../10.0.0.0-IBM-MQC-Redist-LinuxX64"}" && pwd -P)
mkdir -p "$dir/../bin"
"${CXX:-g++}" -std=c++20 -O2 -Wall -Wextra -Wpedantic -pthread \
    -I"$mq/inc" "$dir/main.cpp" -L"$mq/lib64" -Wl,-rpath,"$mq/lib64" -lmqic_r \
    -o "$dir/../bin/mq_traffic"
