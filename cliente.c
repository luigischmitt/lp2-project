/* cliente.c -- cliente TCP da central de reservas.
 *
 *   ./cliente <host> <porta>                  modo interativo (le stdin)
 *   ./cliente <host> <porta> RESERVE 3 luigi  envia um comando e sai
 *
 * O cliente nao conhece o estado compartilhado: fala apenas o protocolo.
 */
#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define LINHA_MAX 1024

typedef struct {
    int    fd;
    char   buf[1024];
    size_t ini, fim;
} leitor;

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

/* A resposta pode chegar picada em varios segmentos TCP; le ate o \n em vez de
 * confiar em um unico read. */
static int le_linha(leitor *l, char *saida, size_t tam)
{
    size_t usado = 0;

    for (;;) {
        if (l->ini == l->fim) {
            ssize_t k = read(l->fd, l->buf, sizeof l->buf);
            if (k < 0) {
                if (errno == EINTR)
                    continue;
                return -1;
            }
            if (k == 0)
                return usado > 0 ? 1 : 0;
            l->ini = 0;
            l->fim = (size_t)k;
        }

        char c = l->buf[l->ini++];
        if (c == '\n') {
            saida[usado] = '\0';
            return 1;
        }
        if (c != '\r' && usado + 1 < tam)
            saida[usado++] = c;
    }
}

static int conecta(const char *host, const char *porta)
{
    struct addrinfo dica, *lista, *p;
    memset(&dica, 0, sizeof dica);
    dica.ai_family = AF_INET;
    dica.ai_socktype = SOCK_STREAM;

    int rc = getaddrinfo(host, porta, &dica, &lista);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rc));
        return -1;
    }

    int fd = -1;
    for (p = lista; p; p = p->ai_next) {
        fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0)
            continue;
        if (connect(fd, p->ai_addr, p->ai_addrlen) == 0)
            break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(lista);

    if (fd < 0)
        perror("connect");
    return fd;
}

/* Envia uma linha e imprime a resposta. Devolve 0 quando o servidor encerrou. */
static int troca(int fd, leitor *l, const char *comando)
{
    char linha[LINHA_MAX];
    int n = snprintf(linha, sizeof linha, "%s\n", comando);
    if (n < 0 || (size_t)n >= sizeof linha) {
        fprintf(stderr, "comando longo demais\n");
        return 1;
    }
    if (escreve_tudo(fd, linha, (size_t)n) < 0) {
        perror("write");
        return 0;
    }

    char resposta[LINHA_MAX];
    int r = le_linha(l, resposta, sizeof resposta);
    if (r <= 0) {
        if (r < 0)
            perror("read");
        return 0;
    }

    puts(resposta);
    fflush(stdout);
    return strcmp(resposta, "BYE") != 0;
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr,
                "uso: %s <host> <porta> [comando...]\n"
                "  sem comando, le comandos do stdin\n", argv[0]);
        return 1;
    }

    int fd = conecta(argv[1], argv[2]);
    if (fd < 0)
        return 1;

    leitor l = { .fd = fd, .ini = 0, .fim = 0 };

    if (argc > 3) {
        /* Comando vindo por argv: junta os argumentos e sai depois da resposta. */
        char comando[LINHA_MAX] = "";
        size_t usado = 0;
        for (int i = 3; i < argc; i++) {
            int n = snprintf(comando + usado, sizeof comando - usado,
                             "%s%s", usado ? " " : "", argv[i]);
            if (n < 0 || (size_t)n >= sizeof comando - usado) {
                fprintf(stderr, "comando longo demais\n");
                close(fd);
                return 1;
            }
            usado += (size_t)n;
        }
        troca(fd, &l, comando);
    } else {
        char entrada[LINHA_MAX];
        while (fgets(entrada, sizeof entrada, stdin)) {
            entrada[strcspn(entrada, "\r\n")] = '\0';
            if (entrada[0] == '\0')
                continue;
            if (!troca(fd, &l, entrada))
                break;
        }
    }

    close(fd);
    return 0;
}
