CC=gcc
CFLAGS=-Wall -g -O3
LDFLAGS= -libverbs

all: ib_server ib_client

ib_server: ib_server.o verbs.o helper.o tcp.o
	$(CC) $(CFLAGS) ib_server.o verbs.o tcp.o helper.o -o ib_server $(LDFLAGS)

ib_client: ib_client.o verbs.o helper.o tcp.o
	$(CC) $(CFLAGS) ib_client.o verbs.o tcp.o helper.o -o ib_client $(LDFLAGS)

ib_client.o: ib_client.c
	${CC} -O3 -c ib_client.c -D_GNU_SOURCE

verbs.o: verbs.c
	${CC} -O3 -c verbs.c -D_GNU_SOURCE

tcp.o: tcp.c
	${CC} -O3 -c tcp.c -D_GNU_SOURCE

helper.o: helper.c
	${CC} -O3 -c helper.c -D_GNU_SOURCE

clean:
	rm -f ib_client ib_server test.o verbs.o tcp.o helper.o ib_client.o ib_server.o
