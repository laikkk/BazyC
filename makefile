INC=/usr/include/postgresql
LIB=/usr/lib
CFLAGS=-I$(INC)
LDLIBS=-L$(LIB) -lpq
CC=gcc

program: main.c
	$(CC) $(CFLAGS) -o program main.c $(LDLIBS)
clean:
	rm -f program
