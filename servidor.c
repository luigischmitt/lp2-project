/* servidor.c -- servidor TCP thread-por-conexao da central de reservas.
 *
 * Nao existe nenhuma primitiva de sincronizacao neste arquivo: todo acesso ao
 * estado compartilhado passa pela API de estado_compartilhado.h, que sincroniza
 * internamente. O servidor e o dono do segmento: cria na inicializacao e
 * remove ao receber SIGINT.
 *
 *   ./servidor <porta> [nome_shm]
 */
#define _POSIX_C_SOURCE 200809L

#include "estado_compartilhado.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define BACKLOG        64
#define LINHA_MAX      512
#define ESPERA_SAIDA_S 2

/* O handler de sinal so pode mexer nisto: escrever em uma flag desse tipo e
 * uma das poucas operacoes async-signal-safe. A limpeza acontece no fluxo
 * principal, depois que o accept e interrompido. */
static volatile sig_atomic_t encerrar = 0;

static int conexoes_ativas = 0;   /* manipulado so por operacoes atomicas */

typedef struct {
    int       fd;
    estado_t *estado;
} conexao;

/* Leitor com buffer proprio: um read do socket pode trazer meia linha ou
 * varias linhas de uma vez, entao nao da para assumir "um read, uma linha". */
typedef struct {
    int    fd;
    char   buf[1024];
    size_t ini, fim;
} leitor;

static void trata_sinal(int s)
{
    (void)s;
    encerrar = 1;
}

static int escreve_tudo(int fd, const char *dado, size_t n)
{
    size_t enviado = 0;
    while (enviado < n) {
        ssize_t k = write(fd, dado + enviado, n - enviado);
        if (k < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        enviado += (size_t)k;
    }
    return 0;
}

static int responde(int fd, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

static int responde(int fd, const char *fmt, ...)
{
    char linha[LINHA_MAX];
    va_list ap;

    va_start(ap, fmt);
    int n = vsnprintf(linha, sizeof linha - 1, fmt, ap);
    va_end(ap);
    if (n < 0)
        return -1;
    if ((size_t)n > sizeof linha - 2)
        n = (int)(sizeof linha - 2);
    linha[n] = '\n';

    return escreve_tudo(fd, linha, (size_t)n + 1);
}

/* Devolve 1 com a linha em saida, 0 no fim da conexao, -1 em erro.
 * *truncada indica que a linha nao coube e o excedente foi descartado. */
static int le_linha(leitor *l, char *saida, size_t tam, int *truncada)
{
    size_t usado = 0;
    *truncada = 0;

    for (;;) {
        if (l->ini == l->fim) {
            ssize_t k = read(l->fd, l->buf, sizeof l->buf);
            if (k < 0) {
                if (errno == EINTR)
                    continue;
                return -1;
            }
            if (k == 0)
                return usado > 0 ? 1 : 0;   /* EOF: entrega o resto sem \n */
            l->ini = 0;
            l->fim = (size_t)k;
        }

        char c = l->buf[l->ini++];
        if (c == '\n') {
            saida[usado] = '\0';
            return 1;
        }
        if (c == '\r')
            continue;
        if (usado + 1 < tam)
            saida[usado++] = c;
        else
            *truncada = 1;
    }
}

/* Converte um token inteiro exigindo que ele seja todo numerico: "12a" e
 * malformado (ERR), diferente de "99" que e um id valido fora de faixa. */
static int para_inteiro(const char *s, int *saida)
{
    if (!s || *s == '\0')
        return 0;

    char *fim;
    errno = 0;
    long v = strtol(s, &fim, 10);
    if (errno != 0 || *fim != '\0' || v < INT32_MIN || v > INT32_MAX)
        return 0;

    *saida = (int)v;
    return 1;
}

static void executa(conexao *c, char *linha)
{
    char *salva = NULL;
    char *cmd = strtok_r(linha, " \t", &salva);

    if (!cmd)
        return;   /* linha em branco: ignora silenciosamente */

    if (strcmp(cmd, "LIST") == 0) {
        if (strtok_r(NULL, " \t", &salva)) {
            responde(c->fd, "ERR LIST nao aceita argumentos");
            return;
        }
        char mapa[ESTADO_N + 1];
        estado_mapa(c->estado, mapa, sizeof mapa);
        responde(c->fd, "MAP %s", mapa);
        return;
    }

    if (strcmp(cmd, "RESERVE") == 0) {
        char *a_id = strtok_r(NULL, " \t", &salva);
        char *titular = strtok_r(NULL, " \t", &salva);
        int id;

        if (!a_id || !titular) {
            responde(c->fd, "ERR uso: RESERVE <id> <titular>");
        } else if (strtok_r(NULL, " \t", &salva)) {
            responde(c->fd, "ERR titular nao pode conter espacos");
        } else if (!para_inteiro(a_id, &id)) {
            responde(c->fd, "ERR id deve ser numerico");
        } else if (strlen(titular) > ESTADO_TITULAR_MAX) {
            responde(c->fd, "ERR titular excede %d bytes", ESTADO_TITULAR_MAX);
        } else {
            switch (estado_reservar(c->estado, id, titular)) {
            case ESTADO_OK:    responde(c->fd, "OK");      break;
            case ESTADO_TAKEN: responde(c->fd, "TAKEN");   break;
            default:           responde(c->fd, "INVALID"); break;
            }
        }
        return;
    }

    if (strcmp(cmd, "CANCEL") == 0) {
        char *a_id = strtok_r(NULL, " \t", &salva);
        int id;

        if (!a_id || strtok_r(NULL, " \t", &salva)) {
            responde(c->fd, "ERR uso: CANCEL <id>");
        } else if (!para_inteiro(a_id, &id)) {
            responde(c->fd, "ERR id deve ser numerico");
        } else {
            switch (estado_cancelar(c->estado, id)) {
            case ESTADO_OK:   responde(c->fd, "OK");      break;
            case ESTADO_FREE: responde(c->fd, "FREE");    break;
            default:          responde(c->fd, "INVALID"); break;
            }
        }
        return;
    }

    if (strcmp(cmd, "STATUS") == 0) {
        char *a_id = strtok_r(NULL, " \t", &salva);
        int id;

        if (!a_id || strtok_r(NULL, " \t", &salva)) {
            responde(c->fd, "ERR uso: STATUS <id>");
        } else if (!para_inteiro(a_id, &id)) {
            responde(c->fd, "ERR id deve ser numerico");
        } else {
            char titular[ESTADO_TITULAR_BUF];
            switch (estado_status(c->estado, id, titular, sizeof titular)) {
            case ESTADO_TAKEN: responde(c->fd, "TAKEN %s", titular); break;
            case ESTADO_FREE:  responde(c->fd, "FREE");              break;
            default:           responde(c->fd, "INVALID");           break;
            }
        }
        return;
    }

    responde(c->fd, "ERR comando desconhecido: %s", cmd);
}

static void *atende(void *arg)
{
    conexao *c = arg;
    leitor l = { .fd = c->fd, .ini = 0, .fim = 0 };
    char linha[LINHA_MAX];
    int truncada;

    while (le_linha(&l, linha, sizeof linha, &truncada) == 1) {
        if (truncada) {
            responde(c->fd, "ERR linha muito longa");
            continue;
        }
        if (strncmp(linha, "QUIT", 4) == 0 &&
            (linha[4] == '\0' || linha[4] == ' ')) {
            responde(c->fd, "BYE");
            break;
        }
        executa(c, linha);
    }

    close(c->fd);
    free(c);
    __atomic_fetch_sub(&conexoes_ativas, 1, __ATOMIC_RELAXED);
    return NULL;
}

static int abre_escuta(unsigned short porta)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    int sim = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sim, sizeof sim);

    struct sockaddr_in end = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_ANY),
        .sin_port = htons(porta),
    };

    if (bind(fd, (struct sockaddr *)&end, sizeof end) < 0 ||
        listen(fd, BACKLOG) < 0) {
        int err = errno;
        close(fd);
        errno = err;
        return -1;
    }
    return fd;
}

static void instala_sinais(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = trata_sinal;
    sigemptyset(&sa.sa_mask);
    /* Sem SA_RESTART de proposito: e isso que faz o accept bloqueado retornar
     * com EINTR para o laco principal notar a flag e sair limpando. */
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Cliente que fecha a conexao no meio de uma resposta nao pode derrubar o
     * servidor; o write falha com EPIPE e a thread encerra sozinha. */
    signal(SIGPIPE, SIG_IGN);
}

/* Da um tempo para as threads em curso terminarem antes de desmapear o
 * segmento, evitando que alguma acesse memoria ja liberada. */
static void aguarda_conexoes(void)
{
    const struct timespec passo = { 0, 50000000 };   /* 50 ms */

    for (int i = 0; i < ESPERA_SAIDA_S * 20; i++) {
        if (__atomic_load_n(&conexoes_ativas, __ATOMIC_RELAXED) == 0)
            return;
        nanosleep(&passo, NULL);
    }
}

int main(int argc, char **argv)
{
    if (argc < 2 || argc > 3) {
        fprintf(stderr, "uso: %s <porta> [nome_shm]\n", argv[0]);
        return 1;
    }

    int porta;
    if (!para_inteiro(argv[1], &porta) || porta < 1 || porta > 65535) {
        fprintf(stderr, "porta invalida: %s\n", argv[1]);
        return 1;
    }
    const char *nome_shm = (argc == 3) ? argv[2] : ESTADO_SHM_PADRAO;

    estado_t *estado = estado_criar(nome_shm);
    if (!estado) {
        perror("estado_criar");
        return 1;
    }

    int escuta = abre_escuta((unsigned short)porta);
    if (escuta < 0) {
        perror("abre_escuta");
        estado_destruir(estado);
        return 1;
    }

    instala_sinais();
    printf("servidor ouvindo na porta %d, segmento %s (%d recursos)\n",
           porta, nome_shm, ESTADO_N);
    fflush(stdout);

    while (!encerrar) {
        struct sockaddr_in cliente;
        socklen_t tam = sizeof cliente;

        int fd = accept(escuta, (struct sockaddr *)&cliente, &tam);
        if (fd < 0) {
            if (errno == EINTR)
                continue;          /* sinal chegou: o while reavalia a flag */
            perror("accept");
            continue;
        }

        conexao *c = malloc(sizeof *c);
        if (!c) {
            close(fd);
            continue;
        }
        c->fd = fd;
        c->estado = estado;

        pthread_t t;
        __atomic_fetch_add(&conexoes_ativas, 1, __ATOMIC_RELAXED);
        if (pthread_create(&t, NULL, atende, c) != 0) {
            __atomic_fetch_sub(&conexoes_ativas, 1, __ATOMIC_RELAXED);
            close(fd);
            free(c);
            continue;
        }
        pthread_detach(t);

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &cliente.sin_addr, ip, sizeof ip);
        printf("conexao de %s:%u\n", ip, ntohs(cliente.sin_port));
        fflush(stdout);
    }

    printf("\nencerrando: liberando segmento %s\n", nome_shm);
    close(escuta);
    aguarda_conexoes();
    estado_destruir(estado);
    return 0;
}
