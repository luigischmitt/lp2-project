#define _POSIX_C_SOURCE 200809L

#include "estado_compartilhado.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MAGICO        0x4C503349u   /* marca de segmento pronto para uso */
#define ESPERA_MS     2000          /* limite de espera pela inicializacao */
#define NOME_MAX      64

/* Layout do segmento compartilhado. Tamanho fixo, sem alocacao dinamica e sem
 * ponteiros: cada processo mapeia o segmento em um endereco diferente, entao
 * um ponteiro gravado aqui seria invalido do outro lado. */
typedef struct {
    pthread_mutex_t mutex;      /* PTHREAD_PROCESS_SHARED */
    unsigned        magico;
    int             n;
    estado_recurso  recurso[ESTADO_N];
} shm_layout;

struct estado {
    shm_layout *shm;
    char        nome[NOME_MAX];
    int         dono;
};

/* ---------------------------------------------------------------- interno */

static int inicializa(shm_layout *m)
{
    pthread_mutexattr_t attr;

    if (pthread_mutexattr_init(&attr) != 0)
        return -1;

    /* O que torna o mutex utilizavel entre processos distintos, e nao apenas
     * entre threads do mesmo processo. */
    if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) != 0) {
        pthread_mutexattr_destroy(&attr);
        return -1;
    }

    int rc = pthread_mutex_init(&m->mutex, &attr);
    pthread_mutexattr_destroy(&attr);
    if (rc != 0) {
        errno = rc;
        return -1;
    }

    m->n = ESTADO_N;
    memset(m->recurso, 0, sizeof m->recurso);

    /* Ultima escrita da inicializacao, com barreira de release: quem enxergar
     * o magico ja enxerga o mutex e o vetor prontos. E o que fecha a janela de
     * corrida com um segundo processo que tenha aberto o mesmo segmento. */
    __atomic_store_n(&m->magico, MAGICO, __ATOMIC_RELEASE);
    return 0;
}

static int espera_pronto(const shm_layout *m)
{
    const struct timespec passo = { 0, 1000000 };   /* 1 ms */

    for (int i = 0; i < ESPERA_MS; i++) {
        if (__atomic_load_n(&m->magico, __ATOMIC_ACQUIRE) == MAGICO)
            return 0;
        nanosleep(&passo, NULL);
    }
    errno = ETIMEDOUT;
    return -1;
}

static estado_t *envelopa(shm_layout *m, const char *nome, int dono)
{
    estado_t *e = calloc(1, sizeof *e);
    if (!e) {
        munmap(m, sizeof *m);
        return NULL;
    }
    e->shm  = m;
    e->dono = dono;
    snprintf(e->nome, sizeof e->nome, "%s", nome);
    return e;
}

static shm_layout *mapeia(int fd)
{
    shm_layout *m = mmap(NULL, sizeof(shm_layout), PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, 0);
    /* O mapeamento sobrevive ao descritor, que ja nao serve para mais nada. */
    close(fd);
    return (m == MAP_FAILED) ? NULL : m;
}

static int valida_titular(const char *t)
{
    if (!t || *t == '\0')
        return 0;
    if (strlen(t) > ESTADO_TITULAR_MAX)
        return 0;
    for (const char *p = t; *p; p++)
        if (*p == ' ' || *p == '\t')
            return 0;
    return 1;
}

/* ------------------------------------------------------- ciclo de vida */

estado_t *estado_criar(const char *nome)
{
    if (!nome)
        nome = ESTADO_SHM_PADRAO;

    int dono = 1;

    /* O_EXCL decide quem inicializa: exatamente um processo vence a criacao. */
    int fd = shm_open(nome, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) {
        if (errno != EEXIST)
            return NULL;
        dono = 0;
        fd = shm_open(nome, O_RDWR, 0600);
        if (fd < 0)
            return NULL;
    } else if (ftruncate(fd, sizeof(shm_layout)) < 0) {
        int err = errno;
        close(fd);
        shm_unlink(nome);
        errno = err;
        return NULL;
    }

    shm_layout *m = mapeia(fd);
    if (!m) {
        if (dono)
            shm_unlink(nome);
        return NULL;
    }

    int rc = dono ? inicializa(m) : espera_pronto(m);
    if (rc < 0) {
        int err = errno;
        munmap(m, sizeof *m);
        if (dono)
            shm_unlink(nome);
        errno = err;
        return NULL;
    }

    return envelopa(m, nome, dono);
}

estado_t *estado_anexar(const char *nome)
{
    if (!nome)
        nome = ESTADO_SHM_PADRAO;

    /* Sem O_CREAT: quem anexa nunca cria nem reinicializa o segmento. */
    int fd = shm_open(nome, O_RDWR, 0600);
    if (fd < 0)
        return NULL;

    shm_layout *m = mapeia(fd);
    if (!m)
        return NULL;

    if (espera_pronto(m) < 0) {
        int err = errno;
        munmap(m, sizeof *m);
        errno = err;
        return NULL;
    }

    return envelopa(m, nome, 0);
}

void estado_fechar(estado_t *e)
{
    if (!e)
        return;
    munmap(e->shm, sizeof(shm_layout));
    free(e);
}

void estado_destruir(estado_t *e)
{
    if (!e)
        return;
    if (e->dono) {
        pthread_mutex_destroy(&e->shm->mutex);
        shm_unlink(e->nome);
    }
    estado_fechar(e);
}

/* ------------------------------------------------- operacoes de dominio */

estado_rc estado_reservar(estado_t *e, int id, const char *titular)
{
    if (id < 0 || id >= ESTADO_N || !valida_titular(titular))
        return ESTADO_INVALID;

    pthread_mutex_lock(&e->shm->mutex);

    /* Verificar e ocupar dentro da mesma regiao critica e o que impede a dupla
     * reserva: nenhuma outra thread ou processo observa o recurso livre entre
     * o teste e a escrita. */
    estado_recurso *r = &e->shm->recurso[id];
    estado_rc rc;
    if (r->ocupado) {
        rc = ESTADO_TAKEN;
    } else {
        r->ocupado = 1;
        snprintf(r->titular, sizeof r->titular, "%s", titular);
        rc = ESTADO_OK;
    }

    pthread_mutex_unlock(&e->shm->mutex);
    return rc;
}

estado_rc estado_cancelar(estado_t *e, int id)
{
    if (id < 0 || id >= ESTADO_N)
        return ESTADO_INVALID;

    pthread_mutex_lock(&e->shm->mutex);

    estado_recurso *r = &e->shm->recurso[id];
    estado_rc rc;
    if (r->ocupado) {
        r->ocupado = 0;
        r->titular[0] = '\0';
        rc = ESTADO_OK;
    } else {
        rc = ESTADO_FREE;
    }

    pthread_mutex_unlock(&e->shm->mutex);
    return rc;
}

estado_rc estado_status(estado_t *e, int id, char *titular_out, size_t tam)
{
    if (id < 0 || id >= ESTADO_N)
        return ESTADO_INVALID;

    pthread_mutex_lock(&e->shm->mutex);

    const estado_recurso *r = &e->shm->recurso[id];
    estado_rc rc;
    if (r->ocupado) {
        if (titular_out && tam > 0)
            snprintf(titular_out, tam, "%s", r->titular);
        rc = ESTADO_TAKEN;
    } else {
        if (titular_out && tam > 0)
            titular_out[0] = '\0';
        rc = ESTADO_FREE;
    }

    pthread_mutex_unlock(&e->shm->mutex);
    return rc;
}

void estado_mapa(estado_t *e, char *buf, size_t tam)
{
    if (!buf || tam == 0)
        return;

    size_t limite = (tam - 1 < (size_t)ESTADO_N) ? tam - 1 : (size_t)ESTADO_N;

    pthread_mutex_lock(&e->shm->mutex);
    for (size_t i = 0; i < limite; i++)
        buf[i] = e->shm->recurso[i].ocupado ? '1' : '0';
    pthread_mutex_unlock(&e->shm->mutex);

    buf[limite] = '\0';
}

void estado_snapshot(estado_t *e, estado_snap *out)
{
    if (!out)
        return;

    /* Copia em bloco sob o lock: a regiao critica dura um memcpy, e o chamador
     * imprime o retrato la fora, ja sem segurar o mutex. */
    pthread_mutex_lock(&e->shm->mutex);
    out->n = e->shm->n;
    memcpy(out->recurso, e->shm->recurso, sizeof out->recurso);
    pthread_mutex_unlock(&e->shm->mutex);
}
