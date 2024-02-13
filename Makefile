.PHONY: run
run: build
	./meinsql

.PHONY: build
build:
	gcc -fdiagnostics-color=always -Wall -Wextra -g src/meinsql.c -o meinsql -std=c2x
