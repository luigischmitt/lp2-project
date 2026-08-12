/* estado_compartilhado.h -- biblioteca-monitor sobre memoria compartilhada POSIX.
 *
 * O estado da central de reservas vive em um segmento POSIX e e acessado por
 * varias threads do servidor e pelo processo inspetor. Toda a sincronizacao
 * acontece dentro desta biblioteca: a API abaixo expoe apenas operacoes de
 * dominio e nenhum chamador ve, adquire ou libera o lock.
 */
#ifndef ESTADO_COMPARTILHADO_H
#define ESTADO_COMPARTILHADO_H

#include <stddef.h>

#define ESTADO_N            64          /* recursos, indexados de 0 a N-1 */
#define ESTADO_TITULAR_MAX  32          /* bytes uteis do nome do titular  */
#define ESTADO_TITULAR_BUF  (ESTADO_TITULAR_MAX + 1)
#define ESTADO_SHM_PADRAO   "/lpii_tp3"

/* Handle opaco. A struct real e definida apenas em estado_compartilhado.c,
 * junto com o mutex: como o tipo e incompleto aqui, nenhum codigo de fora
 * consegue alcancar a primitiva de sincronizacao. A disciplina de monitor
 * passa a ser garantida pelo compilador, e nao por convencao. */
typedef struct estado estado_t;

typedef enum {
    ESTADO_OK = 0,      /* operacao efetivada                       */
    ESTADO_TAKEN,       /* recurso ja estava ocupado                */
    ESTADO_FREE,        /* recurso ja estava livre                  */
    ESTADO_INVALID      /* id fora de faixa ou titular invalido     */
} estado_rc;

typedef struct {
    int  ocupado;
    char titular[ESTADO_TITULAR_BUF];
} estado_recurso;

/* Copia local do estado, devolvida por estado_snapshot: fica na memoria do
 * chamador, fora do segmento, para que ele possa imprimir sem reter o lock. */
typedef struct {
    int            n;
    estado_recurso recurso[ESTADO_N];
} estado_snap;

/* Cria e inicializa o segmento e a primitiva (uso do servidor, dono do
 * segmento). Devolve NULL em erro, com errno posicionado. */
estado_t *estado_criar(const char *nome);

/* Anexa a um segmento ja existente sem reinicializar nada (uso do inspetor).
 * Falha se o segmento nao existir. */
estado_t *estado_anexar(const char *nome);

/* Test-and-set atomico: verifica se esta livre e ocupa sem janela entre as
 * duas coisas. ESTADO_OK, ESTADO_TAKEN ou ESTADO_INVALID. */
estado_rc estado_reservar(estado_t *e, int id, const char *titular);

/* ESTADO_OK, ESTADO_FREE ou ESTADO_INVALID. */
estado_rc estado_cancelar(estado_t *e, int id);

/* ESTADO_TAKEN (com o titular copiado para titular_out), ESTADO_FREE ou
 * ESTADO_INVALID. */
estado_rc estado_status(estado_t *e, int id, char *titular_out, size_t tam);

/* Escreve em buf uma string de ESTADO_N caracteres '0'/'1' mais o terminador.
 * buf precisa comportar ESTADO_N + 1 bytes. */
void estado_mapa(estado_t *e, char *buf, size_t tam);

/* Retrato consistente do estado inteiro, tirado sob o lock. */
void estado_snapshot(estado_t *e, estado_snap *out);

/* Desfaz o mapeamento e libera o handle. Nao remove o segmento. */
void estado_fechar(estado_t *e);

/* Uso do dono: destroi a primitiva, remove o segmento (shm_unlink) e libera o
 * handle. Em um processo que apenas anexou, equivale a estado_fechar. */
void estado_destruir(estado_t *e);

#endif /* ESTADO_COMPARTILHADO_H */
