.PHONY: run
run: build test
	./meinsql run.db

.PHONY: test
test:
	rspec

.PHONY: build
build:
	gcc src/meinsql.c -o meinsql -Werror -Wall -Wextra -g -std=c2x -fdiagnostics-color=always 