all: test run

.PHONY: run
run: build
	./meinsql run.db

.PHONY: test
test:
	rspec

.PHONY: build
build:
	cc src/meinsql.c -o meinsql -Werror -Wall -Wextra -g -std=c2x -fdiagnostics-color=always
