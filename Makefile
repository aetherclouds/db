NAME 	:= meinsql
CC 		:= gcc
CFLAGS 	:= -fms-extensions -std=c23
CFLAGS 	+= -Werror -Wall -Wextra -fdiagnostics-color=always
CRFLAGS += -O3 # release
CDFLAGS += -g # debug


all: $(NAME) test run	

$(NAME): build

run: $(NAME)
	./$(NAMME) run.db

test: $(NAME)
	rspec

build: src/main.c
	$(CC) src/main.c -o meinsql $(CFLAGS) $(CBFLAGS)
	
debug:
	$(CC) src/main.c -o meinsql $(CFLAGS) $(CDFLAGS)
	$(MAKE) test

.PHONY: all run test build debug