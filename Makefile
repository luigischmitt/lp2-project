CC      = gcc
CFLAGS  = -std=c17 -Wall -Wextra -O2 -pthread
LDFLAGS = -pthread
LDLIBS  = -lrt

BINS = servidor cliente inspetor

all: $(BINS)

servidor: servidor.o estado_compartilhado.o
inspetor: inspetor.o estado_compartilhado.o
cliente:  cliente.o

servidor.o inspetor.o estado_compartilhado.o: estado_compartilhado.h

clean:
	rm -f $(BINS) *.o

.PHONY: all clean
