# Compiler settings
CC = gcc
CFLAGS = -g -Wall -std=c17 -I../include

# Libraries
LIBS ?= -lsqlite3
LIBS += -lm

# Target program
PROGRAM = sqlite3-tests

# Object files
OBJS = main.o \
       test-aux.o \
       test-close.o \
       test-open.o \
       test-prepare.o \
       test-wal.o

# Default target
all: $(PROGRAM)

# Test target
test: $(PROGRAM)
	./$(PROGRAM)

# Compile source files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Link program
$(PROGRAM): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS)

# Clean target
clean:
	rm -f $(PROGRAM) $(OBJS)

.PHONY: all test clean
