#!/usr/bin/env bash
# Evidencia do criterio C5: sem dupla reserva sob escritores concorrentes.
# Execute a partir da raiz do projeto, com os binarios ja compilados:
#
#   make && ./testes/concorrencia.sh
#
# Para exercitar o modo bonus (thread pool), passe os argumentos extras do
# servidor em SRV_ARGS:
#
#   PORTA=9600 SHM=/lpii_tp3_pool SRV_ARGS="--pool 4" ./testes/concorrencia.sh
#
# Em macOS, rode dentro do container: ./dev.sh bash -c 'make && ./testes/concorrencia.sh'

set -u

PORTA=${PORTA:-9500}
SHM=${SHM:-/lpii_tp3_teste}
SRV_ARGS=${SRV_ARGS:-}
HOST=127.0.0.1
TMP=$(mktemp -d)
FALHAS=0

limpa() {
    [ -n "${SRV_PID:-}" ] && kill -INT "$SRV_PID" 2>/dev/null
    wait "$SRV_PID" 2>/dev/null
    rm -rf "$TMP"
}
trap limpa EXIT

# wait sem argumentos esperaria tambem pelo servidor, que so termina no SIGINT:
# por isso cada rodada guarda os PIDs dos clientes e espera so por eles.
espera() { for p in "$@"; do wait "$p"; done; }

ok()    { printf '  \033[32mok\033[0m     %s\n' "$1"; }
falha() { printf '  \033[31mFALHOU\033[0m %s\n' "$1"; FALHAS=$((FALHAS + 1)); }

for bin in servidor cliente inspetor; do
    [ -x "./$bin" ] || { echo "faltou compilar: ./$bin (rode make)"; exit 1; }
done

# shellcheck disable=SC2086  # SRV_ARGS precisa ser dividido em palavras
./servidor "$PORTA" "$SHM" $SRV_ARGS >"$TMP/servidor.log" 2>&1 &
SRV_PID=$!
sleep 0.5
kill -0 "$SRV_PID" 2>/dev/null || { echo "servidor nao subiu:"; cat "$TMP/servidor.log"; exit 1; }
head -2 "$TMP/servidor.log"

echo
echo "1) 50 clientes disputando o MESMO recurso"
PIDS=()
for i in $(seq 1 50); do
    ./cliente "$HOST" "$PORTA" RESERVE 0 cliente"$i" >"$TMP/r$i.txt" 2>&1 &
    PIDS+=($!)
done
espera "${PIDS[@]}"
CONFIRMADAS=$(cat "$TMP"/r*.txt | grep -c '^OK$')
RECUSADAS=$(cat "$TMP"/r*.txt | grep -c '^TAKEN$')
echo "     OK=$CONFIRMADAS  TAKEN=$RECUSADAS"
[ "$CONFIRMADAS" -eq 1 ]  && ok "exatamente uma reserva efetivada" \
                          || falha "esperava 1 OK, veio $CONFIRMADAS (dupla reserva!)"
[ "$RECUSADAS" -eq 49 ]   && ok "as outras 49 receberam TAKEN" \
                          || falha "esperava 49 TAKEN, veio $RECUSADAS"

TITULAR=$(./cliente "$HOST" "$PORTA" STATUS 0)
case "$TITULAR" in
    "TAKEN cliente"*) ok "titular gravado sem corromper: $TITULAR" ;;
    *)                falha "STATUS 0 devolveu '$TITULAR'" ;;
esac

echo
echo "2) 64 clientes em recursos distintos, todos de uma vez"
./cliente "$HOST" "$PORTA" CANCEL 0 >/dev/null
PIDS=()
for i in $(seq 0 63); do
    ./cliente "$HOST" "$PORTA" RESERVE "$i" p"$i" >"$TMP/d$i.txt" 2>&1 &
    PIDS+=($!)
done
espera "${PIDS[@]}"
TOTAL_OK=$(cat "$TMP"/d*.txt | grep -c '^OK$')
MAPA=$(./cliente "$HOST" "$PORTA" LIST)
UNS=$(printf '%s' "${MAPA#MAP }" | tr -cd '1' | wc -c | tr -d ' ')
echo "     OK=$TOTAL_OK  uns no mapa=$UNS"
[ "$TOTAL_OK" -eq 64 ] && ok "as 64 reservas foram efetivadas" \
                       || falha "esperava 64 OK, veio $TOTAL_OK"
[ "$UNS" -eq 64 ]      && ok "LIST reflete todas as escritas (sem perda de atualizacao)" \
                       || falha "esperava 64 uns no mapa, veio $UNS"

echo
echo "3) inspetor lendo o segmento durante carga concorrente"
PIDS=()
for i in $(seq 0 63); do
    ( ./cliente "$HOST" "$PORTA" CANCEL "$i" >/dev/null
      ./cliente "$HOST" "$PORTA" RESERVE "$i" q"$i" >/dev/null ) &
    PIDS+=($!)
done
./inspetor "$SHM" >"$TMP/inspetor.txt" 2>&1
INSPETOR_RC=$?
espera "${PIDS[@]}"
LINHA=$(grep -E '^recursos:' "$TMP/inspetor.txt")
SOMA=$(echo "$LINHA" | awk '{print $4 + $6}')
echo "     $LINHA"
[ "$INSPETOR_RC" -eq 0 ] && ok "inspetor anexou ao segmento sem socket" \
                         || falha "inspetor saiu com codigo $INSPETOR_RC"
[ "${SOMA:-0}" -eq 64 ]  && ok "snapshot coerente: ocupados + livres = 64" \
                         || falha "snapshot inconsistente (soma=$SOMA)"

echo
echo "4) limpeza no SIGINT com um cliente ocioso conectado"
# Conexao aberta que nao envia nada: e o caso que travaria o encerramento se
# uma thread ficasse presa para sempre no read.
exec 3<>"/dev/tcp/$HOST/$PORTA"
kill -INT "$SRV_PID"

INICIO=$SECONDS
while kill -0 "$SRV_PID" 2>/dev/null && [ $((SECONDS - INICIO)) -lt 10 ]; do
    sleep 0.2
done
exec 3<&-

if kill -0 "$SRV_PID" 2>/dev/null; then
    falha "servidor nao encerrou em 10s (thread presa no read?)"
    kill -9 "$SRV_PID" 2>/dev/null
else
    ok "servidor encerrou em $((SECONDS - INICIO))s mesmo com cliente ocioso"
fi
wait "$SRV_PID" 2>/dev/null
SRV_PID=
if [ -e "/dev/shm${SHM}" ]; then
    falha "segmento orfao em /dev/shm${SHM}"
else
    ok "segmento removido, nada orfao em /dev/shm"
fi

echo
if [ "$FALHAS" -eq 0 ]; then
    printf '\033[32mtodos os testes passaram\033[0m\n'
else
    printf '\033[31m%d verificacao(oes) falharam\033[0m\n' "$FALHAS"
fi
exit "$FALHAS"
