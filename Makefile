CC ?= gcc
CFLAGS = -fms-extensions -std=c23
CFLAGS += -Werror -Wall -Wextra -fdiagnostics-color=always
# CFLAGS+=-g
CFLAGS += -O3


all: build test run	

run: build
	./meinsql run.db

test:
	rspec

build:
	$(CC) src/meinsql.c -o meinsql $(CFLAGS)

.PHONY: all run test build