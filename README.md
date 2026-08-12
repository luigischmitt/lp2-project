# TP3 — Central de Reservas

Trabalho Prático 3 · Módulo 3 (Comunicação) · Linguagem de Programação II · UFPB

**Cenário escolhido: B — Central de reservas.** N = 64 recursos indexados de 0 a 63,
reservados e cancelados por clientes concorrentes. O estado vive em um segmento de
memória compartilhada POSIX e é acessado por múltiplas threads do servidor e por um
segundo processo, o inspetor.

## Arquivos

| Arquivo | Papel |
|---|---|
| `estado_compartilhado.h` / `.c` | biblioteca-monitor: o segmento, a primitiva e a API de domínio |
| `servidor.c` | servidor TCP thread-por-conexão, dono do segmento |
| `cliente.c` | cliente TCP interativo ou por argv |
| `inspetor.c` | processo separado que anexa à mesma SHM, sem socket |
| `testes/concorrencia.sh` | prova de que não há dupla reserva nem segmento órfão |
| `dev.sh` | atalho para build e execução em container Linux |

## Build e execução

Requer Linux com gcc (C17). O `Makefile` gera exatamente `servidor`, `cliente` e
`inspetor`, com `-Wall -Wextra`:

```sh
make
```

Em três terminais:

```sh
./servidor 9000                    # porta obrigatória, nome da SHM opcional
./cliente 127.0.0.1 9000           # interativo, lê comandos do stdin
./inspetor                         # snapshot do estado, sem passar por socket
```

O nome padrão do segmento é `/lpii_tp3` e pode ser sobrescrito por argumento, o que
importa em máquinas compartilhadas:

```sh
./servidor 9000 /lpii_tp3_20240001
./inspetor /lpii_tp3_20240001
```

O cliente também aceita um comando direto por argv, útil em scripts:

```sh
./cliente 127.0.0.1 9000 RESERVE 12 luigi
```

Encerre o servidor com `Ctrl+C`: ele destrói a primitiva e remove o segmento.

### Nota sobre macOS

macOS não implementa `PTHREAD_PROCESS_SHARED` (`pthread_mutexattr_setpshared` retorna
`ENOTSUP`), então o desenvolvimento foi feito em container Linux. O `dev.sh` encapsula
isso:

```sh
./dev.sh bash -c 'make && ./testes/concorrencia.sh'
```

## Protocolo

Uma requisição por linha, campos separados por espaço, terminada em `\n`.

| Comando | Resposta |
|---|---|
| `LIST` | `MAP <64 caracteres 0/1>` (`0` = livre, `1` = ocupado) |
| `RESERVE <id> <titular>` | `OK` se reservou; `TAKEN` se já ocupado; `INVALID` se o id está fora de faixa |
| `CANCEL <id>` | `OK` se liberou; `FREE` se já estava livre; `INVALID` se o id está fora de faixa |
| `STATUS <id>` | `FREE`; `TAKEN <titular>`; ou `INVALID` |
| `QUIT` | `BYE` e fecha a conexão |

Linha malformada responde `ERR <motivo>`. A distinção é proposital: `INVALID` é um id
numérico fora de faixa (por exemplo `99`), enquanto `ERR` é uma requisição que sequer
faz sentido (comando desconhecido, id não numérico, argumento faltando, titular com
espaço ou maior que 32 bytes).

Exemplo de sessão:

```
RESERVE 5 luigi     -> OK
RESERVE 5 maria     -> TAKEN
STATUS 5            -> TAKEN luigi
LIST                -> MAP 0000010000000000000000000000000000000000000000000000000000000000
CANCEL 5            -> OK
CANCEL 5            -> FREE
RESERVE 99 joao     -> INVALID
RESERVE abc joao    -> ERR id deve ser numerico
QUIT                -> BYE
```

## A biblioteca-monitor

### O lock é inalcançável de fora

`estado_t` é um tipo **opaco**: o header declara `typedef struct estado estado_t;` e a
definição real — a única que contém o `pthread_mutex_t` — existe apenas no `.c`. Como o
tipo é incompleto fora da biblioteca, nenhum código do servidor ou do inspetor consegue
acessar a primitiva, mesmo por engano. A disciplina de monitor deixa de ser uma
convenção e passa a ser verificada pelo compilador.

A API expõe só operações de domínio: `estado_criar`, `estado_anexar`,
`estado_reservar`, `estado_cancelar`, `estado_status`, `estado_mapa`,
`estado_snapshot`, `estado_fechar` e `estado_destruir`. Cada uma adquire e libera o
lock internamente.

### O segmento

`shm_open` + `ftruncate` + `mmap` sobre uma struct de tamanho fixo, sem alocação
dinâmica e sem nenhum ponteiro armazenado dentro do segmento — um ponteiro seria
inválido no outro processo, já que cada um mapeia o segmento em um endereço diferente.

```c
typedef struct {
    pthread_mutex_t mutex;      /* PTHREAD_PROCESS_SHARED */
    unsigned        magico;
    int             n;
    estado_recurso  recurso[64];   /* { int ocupado; char titular[33]; } */
} shm_layout;
```

### Corrida de inicialização

O servidor abre o segmento com `O_CREAT | O_EXCL`, o que elege exatamente um
inicializador mesmo que dois servidores subam ao mesmo tempo. Quem vence faz
`ftruncate`, mapeia, inicializa o mutex com `pthread_mutexattr_setpshared` e, **como
última operação**, grava o campo `magico` com uma barreira de release. Quem perde a
disputa (ou o inspetor, que abre sem `O_CREAT`) espera o mágico aparecer com uma
barreira de acquire antes de usar o mutex.

Gravar o mágico por último é o que fecha a janela: é impossível observar um segmento
parcialmente inicializado, porque o marcador só fica visível depois que tudo o que ele
protege já está pronto. Um segmento POSIX recém-criado vem zerado, então o valor
mágico nunca aparece por acidente.

### Limpeza

O handler de `SIGINT`/`SIGTERM` faz uma coisa só: escrever em uma
`volatile sig_atomic_t`, que é das poucas operações async-signal-safe. O `sigaction` é
instalado **sem** `SA_RESTART` de propósito — assim o `accept` bloqueado retorna com
`EINTR`, o laço principal observa a flag e sai. A limpeza (`pthread_mutex_destroy` e
`shm_unlink`) acontece no fluxo normal, nunca dentro do handler. Antes de desmapear, o
servidor aguarda até 2 segundos pelas conexões em curso, para que nenhuma thread toque
memória já liberada.

`SIGPIPE` é ignorado: um cliente que desaparece no meio de uma resposta faz o `write`
falhar com `EPIPE` e a thread encerra sozinha, sem derrubar o servidor.

## Justificativa da primitiva

**Escolha: um `pthread_mutex_t` process-shared vivendo dentro do segmento.**

*Perfil de acesso.* Numa bilheteria, as escritas não são raras: `RESERVE` e `CANCEL`
são a razão de o sistema existir, e disputam os mesmos recursos que `LIST` e `STATUS`
leem. Não é um cenário de leitura dominante. As regiões críticas, por outro lado, são
minúsculas — comparar um inteiro e copiar no máximo 32 bytes — e o pior caso, o
`snapshot`, é um `memcpy` de pouco mais de 2 KB.

*Por que não um rwlock.* `RESERVE` é um **test-and-set**: verificar se o recurso está
livre e ocupá-lo precisam ser indivisíveis. Com leitores-escritores, duas threads
poderiam segurar o lock de leitura ao mesmo tempo, ambas observar `ocupado == 0` e só
então tentar escrever — e o resultado seria exatamente a dupla reserva que o cenário
proíbe. Fazer a verificação já sob o lock de escrita elimina o paralelismo entre
leitores que era o argumento a favor do rwlock, e ainda por cima traria o risco de
starvation de escritores num sistema onde escrever é o caso de uso principal.

*Por que um lock único basta.* Com 64 recursos e regiões críticas na casa de dezenas de
nanossegundos, a contenção é irrelevante diante do custo de uma ida e volta pela rede.
Um lock por recurso multiplicaria por 64 o estado de sincronização e tornaria o
snapshot consistente bem mais difícil, sem ganho mensurável.

*Por que não uma variável de condição.* O protocolo canônico responde `TAKEN`
imediatamente; não existe operação que precise bloquear até um recurso ser liberado.
Uma condvar seria estrutura sem cliente.

O mutex mora fisicamente dentro do segmento, com
`pthread_mutexattr_setpshared(PTHREAD_PROCESS_SHARED)`, e é por isso que serve tanto às
threads do servidor quanto ao processo inspetor.

## Por que SHM e não troca de mensagens

O inspetor é a resposta prática. Ele não fala o protocolo, não abre socket e não pede
nada ao servidor: mapeia o mesmo segmento e lê o estado vivo com custo zero de cópia.
Se o estado fosse privado do servidor e exposto só por mensagens, todo observador novo
exigiria uma conexão, uma rodada de requisição/resposta e uma cópia serializada dos
dados. Com SHM, o custo de um segundo processo observador é um `mmap`.

## Testes de concorrência

```sh
make && ./testes/concorrencia.sh
```

```
1) 50 clientes disputando o MESMO recurso
     OK=1  TAKEN=49
2) 64 clientes em recursos distintos, todos de uma vez
     OK=64  uns no mapa=64
3) inspetor lendo o segmento durante carga concorrente
     recursos: 64   ocupados: 55   livres: 9
4) limpeza no SIGINT
     segmento removido, nada orfao em /dev/shm

todos os testes passaram
```

O teste 1 é a verificação de dupla reserva: 50 clientes simultâneos no mesmo assento
produzem exatamente um `OK`. O teste 3 mostra que o snapshot nunca pega o estado pela
metade — mesmo com 64 pares `CANCEL`/`RESERVE` em curso, ocupados + livres = 64.

A biblioteca também foi exercitada diretamente, fora da rede, com 8 processos × 16
threads disputando os 64 recursos: exatamente 64 reservas efetivadas.
