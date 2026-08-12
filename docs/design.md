# TP3 — Central de Reservas (Cenário B) · Documento de Design

Disciplina: LPII · UFPB · Módulo 3 — Comunicação
Cenário escolhido: **B — Central de reservas** (N = 64 recursos)

Este documento registra as decisões de projeto antes da implementação. O README.md
final é o documento de entrega; este aqui é o raciocínio por trás dele.

## 1. Visão geral

Quatro artefatos compartilham um único estado que vive em memória compartilhada POSIX:

```
                    ┌──────────────────────────────┐
   cliente ──TCP──▶ │ servidor (thread/conexão)    │──┐
   cliente ──TCP──▶ │                              │  │  chama só a API
                    └──────────────────────────────┘  │
                                                       ▼
                                        ┌───────────────────────────┐
                                        │ estado_compartilhado (lib)│
                                        │  mutex pshared + dados    │
                                        └───────────────────────────┘
                                                       ▲
   inspetor ──── anexa direto à SHM (sem socket) ──────┘
```

O servidor é dono do segmento: cria, inicializa e, ao receber SIGINT, destrói.
O inspetor apenas anexa a um segmento já existente.

## 2. A biblioteca-monitor

### 2.1 Tipo opaco

O header declara `typedef struct estado estado_t;` sem definir a struct. A definição
real — a única que contém o `pthread_mutex_t` — vive no `.c`. Consequência: nenhum
chamador consegue acessar o lock, porque o tipo é incompleto fora da biblioteca.
A disciplina de monitor deixa de ser convenção e passa a ser garantida pelo compilador.

### 2.2 Layout do segmento

Tamanho fixo, sem alocação dinâmica e sem ponteiros dentro do segmento (um ponteiro
seria inválido em outro processo, já que o endereço de mapeamento difere).

```c
#define ESTADO_N        64   /* recursos, indexados de 0 a N-1 */
#define ESTADO_TITULAR  33   /* 32 bytes + terminador           */

typedef struct { int ocupado; char titular[ESTADO_TITULAR]; } recurso_t;

struct estado_shm {          /* isto é o que mora na SHM */
    pthread_mutex_t mutex;   /* PTHREAD_PROCESS_SHARED   */
    unsigned magico;         /* marca "inicialização concluída" */
    int      n;
    recurso_t r[ESTADO_N];
};
```

### 2.3 API (só operações de domínio)

| Função | Papel |
|---|---|
| `estado_criar(nome)` | servidor: cria/inicializa o segmento e o mutex |
| `estado_anexar(nome)` | inspetor: anexa a segmento existente, nunca reinicializa |
| `estado_reservar(e, id, titular)` | test-and-set atômico → `OK` / `TAKEN` / `INVALID` |
| `estado_cancelar(e, id)` | → `OK` / `FREE` / `INVALID` |
| `estado_status(e, id, out)` | → `FREE` / `TAKEN <titular>` / `INVALID` |
| `estado_mapa(e, buf)` | string de N caracteres `0`/`1` |
| `estado_snapshot(e, out)` | cópia consistente do array inteiro sob o lock |
| `estado_fechar(e)` | `munmap` + `close` |
| `estado_destruir(e)` | servidor: destrói o mutex, `shm_unlink`, fecha |

Toda função que lê ou escreve o estado trava e destrava internamente. O snapshot
copia para memória local do chamador, então o inspetor imprime fora da região crítica —
o lock fica retido pelo tempo de um `memcpy`, não pelo tempo de um `printf`.

### 2.4 Corrida de inicialização

O servidor abre com `O_CREAT | O_EXCL`:

- **Sucesso** → é o dono: `ftruncate`, `mmap`, inicializa o mutex com
  `pthread_mutexattr_setpshared(PTHREAD_PROCESS_SHARED)`, zera o array e **por último**
  grava `magico`.
- **`EEXIST`** → reabre sem `O_EXCL` e espera `magico` aparecer antes de usar o mutex.

Gravar `magico` como última operação é o que fecha a janela: nenhum processo consegue
observar um segmento parcialmente inicializado. O inspetor abre sem `O_CREAT` e falha
com mensagem clara se o segmento não existir.

## 3. Justificativa da primitiva: mutex, não rwlock

`RESERVE` é um **test-and-set**: verificar se o recurso está livre e ocupá-lo precisam
ser uma única operação indivisível. Com um rwlock, duas threads poderiam adquirir o
lock de leitura simultaneamente, ambas observar `ocupado == 0`, e então promover para
escrita — resultando em dupla reserva, exatamente o defeito que o cenário proíbe.

O perfil de acesso reforça a escolha: as operações de escrita (`RESERVE`, `CANCEL`) são
tão frequentes quanto as de leitura (`LIST`, `STATUS`) numa bilheteria, e as regiões
críticas são curtíssimas (comparar um int e copiar ≤32 bytes). O ganho teórico de
paralelismo entre leitores não compensaria o custo maior de aquisição do rwlock nem a
complexidade de tratar starvation de escritores.

Um único mutex protegendo o segmento inteiro basta: com N = 64 e regiões críticas na
casa de nanossegundos, a contenção é irrelevante e o código fica auditável.

## 4. Protocolo

Uma requisição por linha, terminada em `\n`.

| Comando | Resposta |
|---|---|
| `LIST` | `MAP <64 caracteres 0/1>` |
| `RESERVE <id> <titular>` | `OK` / `TAKEN` / `INVALID` |
| `CANCEL <id>` | `OK` / `FREE` / `INVALID` |
| `STATUS <id>` | `FREE` / `TAKEN <titular>` / `INVALID` |
| `QUIT` | `BYE` e fecha a conexão |

Linha malformada (comando desconhecido, argumento faltando, id não numérico, titular
com mais de 32 bytes) responde `ERR <motivo>`.

## 5. Servidor

TCP com `SO_REUSEADDR`, `accept` em laço, uma thread destacada por conexão. O
`servidor.c` não contém nenhuma primitiva de sincronização — toda mutação de estado
passa pela API da biblioteca.

Encerramento: `sigaction` **sem** `SA_RESTART` instala um handler que apenas marca uma
`volatile sig_atomic_t`. O `accept` bloqueado retorna `EINTR`, o laço observa a flag e
sai; a limpeza (`estado_destruir`) acontece no fluxo principal, não no handler, o que
respeita as restrições de async-signal-safety.

## 6. Verificação de concorrência

Script `testes/concorrencia.sh`:

1. 50 clientes simultâneos reservando **o mesmo id** → exatamente 1 `OK` e 49 `TAKEN`.
2. 64 clientes simultâneos em ids distintos → `LIST` retorna 64 caracteres `1`.
3. `inspetor` executado durante a carga → snapshot sempre coerente.

## 7. Ambiente

macOS não implementa `PTHREAD_PROCESS_SHARED` (retorna `ENOTSUP`), então build e testes
rodam em container Linux (`gcc:13`) com o repositório montado como volume.

## 8. Bônus (só se sobrar tempo)

Thread pool com fila circular de descritores protegida por mutex + duas variáveis de
condição (`nao_vazia` / `nao_cheia`), substituindo o `pthread_create` por conexão.
