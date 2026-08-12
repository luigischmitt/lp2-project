/* servidor.c -- servidor TCP da central de reservas.
 *
 * Nao existe nenhuma primitiva de sincronizacao sobre o estado compartilhado
 * neste arquivo: todo acesso passa pela API de estado_compartilhado.h, que
 * sincroniza internamente. O servidor e o dono do segmento: cria na
 * inicializacao e remove ao receber SIGINT.
 *
 *   ./servidor <porta> [nome_shm] [--pool [N]]
 *
 * Por padrao usa thread-por-conexao. Com --pool, um numero fixo de workers
 * consome uma fila de conexoes (bonus); o mutex e as condvars da fila sao
 * locais ao processo e nao tem relacao com a sincronizacao da SHM.
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
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define BACKLOG          64
#define LINHA_MAX        512
#define ESPERA_SAIDA_S   2
#define FILA_CAP         128
#define WORKERS_PADRAO   8
#define WORKERS_MAX      256

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

/* Fila de conexoes do modo --pool: buffer circular classico de
 * produtor-consumidor. A thread do accept e a unica produtora, os workers sao
 * os consumidores. Duas condvars separadas evitam acordar quem nao tem o que
 * fazer: quem espera vaga nao e acordado por uma insercao. */
static struct {
    int             fd[FILA_CAP];
    size_t          ini, qtd;
    pthread_mutex_t mutex;
    pthread_cond_t  nao_vazia;
    pthread_cond_t  nao_cheia;
    int             fechada;
} fila = {
    .ini = 0, .qtd = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .nao_vazia = PTHREAD_COND_INITIALIZER,
    .nao_cheia = PTHREAD_COND_INITIALIZER,
    .fechada = 0,
};

static void trata_sinal(int s)
{
    (void)s;
    encerrar = 1;
}

static void fila_poe(int fd)
{
    pthread_mutex_lock(&fila.mutex);

    while (fila.qtd == FILA_CAP && !fila.fechada)
        pthread_cond_wait(&fila.nao_cheia, &fila.mutex);

    if (fila.fechada) {
        pthread_mutex_unlock(&fila.mutex);
        close(fd);
        return;
    }

    fila.fd[(fila.ini + fila.qtd) % FILA_CAP] = fd;
    fila.qtd++;
    pthread_cond_signal(&fila.nao_vazia);
    pthread_mutex_unlock(&fila.mutex);
}

/* Devolve 1 com uma conexao, ou 0 quando a fila esvaziou e foi fechada. */
static int fila_tira(int *fd)
{
    pthread_mutex_lock(&fila.mutex);

    while (fila.qtd == 0 && !fila.fechada)
        pthread_cond_wait(&fila.nao_vazia, &fila.mutex);

    if (fila.qtd == 0) {          /* so acontece com a fila fechada */
        pthread_mutex_unlock(&fila.mutex);
        return 0;
    }

    *fd = fila.fd[fila.ini];
    fila.ini = (fila.ini + 1) % FILA_CAP;
    fila.qtd--;
    pthread_cond_signal(&fila.nao_cheia);
    pthread_mutex_unlock(&fila.mutex);
    return 1;
}

/* Os workers drenam o que sobrou na fila e so entao encerram. */
static void fila_fecha(void)
{
    pthread_mutex_lock(&fila.mutex);
    fila.fechada = 1;
    pthread_cond_broadcast(&fila.nao_vazia);
    pthread_cond_broadcast(&fila.nao_cheia);
    pthread_mutex_unlock(&fila.mutex);
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
                /* SO_RCVTIMEO expirou: e a chance de um cliente ocioso notar
                 * que o servidor esta encerrando, em vez de segurar a thread
                 * (e o shutdown) indefinidamente. */
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    if (encerrar)
                        return -1;
                    continue;
                }
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

/* Um sinal dirigido ao processo e entregue a qualquer thread que nao o
 * bloqueie. Bloqueando SIGINT/SIGTERM nas threads de trabalho, a entrega recai
 * sobre a thread principal, que e quem precisa acordar do accept. */
static void bloqueia_sinais(void)
{
    sigset_t conjunto;
    sigemptyset(&conjunto);
    sigaddset(&conjunto, SIGINT);
    sigaddset(&conjunto, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &conjunto, NULL);
}

/* Atende uma conexao ate o fim. Igual nos dois modos: a diferenca entre
 * thread-por-conexao e pool esta so em quem chama esta funcao. */
static void atende_conexao(int fd, estado_t *estado)
{
    conexao c = { .fd = fd, .estado = estado };
    leitor l = { .fd = fd, .ini = 0, .fim = 0 };
    char linha[LINHA_MAX];
    int truncada;

    __atomic_fetch_add(&conexoes_ativas, 1, __ATOMIC_RELAXED);

    while (le_linha(&l, linha, sizeof linha, &truncada) == 1) {
        if (truncada) {
            responde(fd, "ERR linha muito longa");
            continue;
        }
        if (strncmp(linha, "QUIT", 4) == 0 &&
            (linha[4] == '\0' || linha[4] == ' ')) {
            responde(fd, "BYE");
            break;
        }
        executa(&c, linha);
    }

    close(fd);
    __atomic_fetch_sub(&conexoes_ativas, 1, __ATOMIC_RELAXED);
}

static void *thread_conexao(void *arg)
{
    conexao *c = arg;
    bloqueia_sinais();
    atende_conexao(c->fd, c->estado);
    free(c);
    return NULL;
}

static void *thread_worker(void *arg)
{
    estado_t *estado = arg;
    int fd;

    bloqueia_sinais();
    while (fila_tira(&fd))
        atende_conexao(fd, estado);

    return NULL;
}

static int abre_escuta(unsigned short porta)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    int sim = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sim, sizeof sim);

    /* Faz o accept expirar de tempos em tempos. O caminho normal de
     * encerramento e o EINTR provocado pelo sinal; isto e a rede de seguranca
     * para o caso de o sinal ter sido tratado enquanto a thread principal
     * estava fora do accept. */
    struct timeval espera = { .tv_sec = 1, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &espera, sizeof espera);

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
    const char *nome_shm = ESTADO_SHM_PADRAO;
    int porta = -1, usa_pool = 0, workers = WORKERS_PADRAO, shm_definido = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--pool") == 0) {
            usa_pool = 1;
            int n;
            if (i + 1 < argc && para_inteiro(argv[i + 1], &n)) {
                if (n < 1 || n > WORKERS_MAX) {
                    fprintf(stderr, "numero de workers invalido: %d\n", n);
                    return 1;
                }
                workers = n;
                i++;
            }
        } else if (porta < 0) {
            if (!para_inteiro(argv[i], &porta) || porta < 1 || porta > 65535) {
                fprintf(stderr, "porta invalida: %s\n", argv[i]);
                return 1;
            }
        } else if (!shm_definido) {
            nome_shm = argv[i];
            shm_definido = 1;
        } else {
            fprintf(stderr, "argumento inesperado: %s\n", argv[i]);
            return 1;
        }
    }

    if (porta < 0) {
        fprintf(stderr, "uso: %s <porta> [nome_shm] [--pool [N]]\n", argv[0]);
        return 1;
    }

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

    pthread_t *equipe = NULL;
    if (usa_pool) {
        equipe = calloc((size_t)workers, sizeof *equipe);
        if (!equipe) {
            perror("calloc");
            close(escuta);
            estado_destruir(estado);
            return 1;
        }
        for (int i = 0; i < workers; i++) {
            if (pthread_create(&equipe[i], NULL, thread_worker, estado) != 0) {
                perror("pthread_create");
                workers = i;   /* segue com os que subiram */
                break;
            }
        }
        if (workers == 0) {
            fprintf(stderr, "nenhum worker pode ser criado\n");
            free(equipe);
            close(escuta);
            estado_destruir(estado);
            return 1;
        }
    }

    printf("servidor ouvindo na porta %d, segmento %s (%d recursos), modo %s\n",
           porta, nome_shm, ESTADO_N,
           usa_pool ? "thread pool" : "thread-por-conexao");
    if (usa_pool)
        printf("pool: %d workers, fila com capacidade %d\n", workers, FILA_CAP);
    fflush(stdout);

    while (!encerrar) {
        struct sockaddr_in cliente;
        socklen_t tam = sizeof cliente;

        int fd = accept(escuta, (struct sockaddr *)&cliente, &tam);
        if (fd < 0) {
            /* EINTR: o sinal chegou. EAGAIN: o SO_RCVTIMEO expirou. Em ambos
             * os casos basta deixar o while reavaliar a flag de encerramento. */
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            perror("accept");
            continue;
        }

        struct timeval espera = { .tv_sec = 1, .tv_usec = 0 };
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &espera, sizeof espera);

        if (usa_pool) {
            fila_poe(fd);
        } else {
            conexao *c = malloc(sizeof *c);
            if (!c) {
                close(fd);
                continue;
            }
            c->fd = fd;
            c->estado = estado;

            pthread_t t;
            if (pthread_create(&t, NULL, thread_conexao, c) != 0) {
                close(fd);
                free(c);
                continue;
            }
            pthread_detach(t);
        }

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &cliente.sin_addr, ip, sizeof ip);
        printf("conexao de %s:%u\n", ip, ntohs(cliente.sin_port));
        fflush(stdout);
    }

    printf("\nencerrando: liberando segmento %s\n", nome_shm);
    close(escuta);

    if (usa_pool) {
        /* Fechar a fila acorda os workers parados na condvar; eles drenam o
         * que sobrou e retornam, entao o join termina. */
        fila_fecha();
        for (int i = 0; i < workers; i++)
            pthread_join(equipe[i], NULL);
        free(equipe);
    }

    aguarda_conexoes();
    estado_destruir(estado);
    return 0;
}
