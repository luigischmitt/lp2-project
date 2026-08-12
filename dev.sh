#!/bin/sh
# Build e execucao em Linux. Necessario porque macOS nao implementa
# PTHREAD_PROCESS_SHARED (pthread_mutexattr_setpshared retorna ENOTSUP).
#
#   ./dev.sh              -> shell interativo no container
#   ./dev.sh make         -> compila
#   ./dev.sh ./servidor 9000
set -e

IMAGEM=gcc:13

if [ $# -eq 0 ]; then
    set -- bash
fi

exec docker run --rm -it -v "$PWD":/app -w /app "$IMAGEM" "$@"
