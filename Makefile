run: build
	./meinsql

build:
	gcc -fdiagnostics-color=always -Wall -Wextra -g meinsql.c -o meinsql.out
