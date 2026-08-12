/* inspetor.c -- le o estado vivo anexando ao mesmo segmento, sem socket.
 *
 *   ./inspetor [nome_shm]
 *
 * E a evidencia de que o estado e realmente interprocessos: um segundo
 * processo enxerga o que o servidor escreveu sem copia e sem protocolo, so
 * mapeando o mesmo segmento. Nao cria nem reinicializa nada, e nao toca o
 * lock: o retrato vem de estado_snapshot, que sincroniza por dentro.
 */
#define _POSIX_C_SOURCE 200809L

#include "estado_compartilhado.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv)
{
    if (argc > 2) {
        fprintf(stderr, "uso: %s [nome_shm]\n", argv[0]);
        return 1;
    }
    const char *nome = (argc == 2) ? argv[1] : ESTADO_SHM_PADRAO;

    estado_t *e = estado_anexar(nome);
    if (!e) {
        if (errno == ENOENT)
            fprintf(stderr,
                    "segmento %s nao existe -- o servidor esta no ar?\n", nome);
        else
            perror("estado_anexar");
        return 1;
    }

    estado_snap s;
    estado_snapshot(e, &s);
    estado_fechar(e);   /* o retrato ja e uma copia local: solta o segmento */

    int ocupados = 0;
    for (int i = 0; i < s.n; i++)
        ocupados += (s.recurso[i].ocupado != 0);

    printf("segmento: %s\n", nome);
    printf("recursos: %d   ocupados: %d   livres: %d\n\n",
           s.n, ocupados, s.n - ocupados);

    printf("mapa (0 = livre, 1 = ocupado)\n");
    for (int i = 0; i < s.n; i++) {
        if (i % 16 == 0)
            printf("  %02d  ", i);
        putchar(s.recurso[i].ocupado ? '1' : '0');
        if (i % 16 == 15)
            putchar('\n');
    }
    if (s.n % 16 != 0)
        putchar('\n');

    if (ocupados == 0) {
        printf("\nnenhuma reserva ativa\n");
        return 0;
    }

    printf("\nreservas ativas\n");
    for (int i = 0; i < s.n; i++)
        if (s.recurso[i].ocupado)
            printf("  %02d  %s\n", i, s.recurso[i].titular);

    return 0;
}
