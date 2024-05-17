#pragma once

#define _POSIX_C_SOURCE 200809L

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <getopt.h>


// NOTE: requires c2x standard
// `__VA_OPT__` gets replaced with its argument if variadic arguments (e.g. `...`) are present
#define print_success(format, ...) (use_color ? printf("\x1b[32m" format "\x1b[39m\n" __VA_OPT__(,) __VA_ARGS__) : printf(format"\n" __VA_OPT__(,) __VA_ARGS__))
#define print_error(format, ...) (use_color ? printf("\x1b[31m" format "\x1b[39m\n" __VA_OPT__(,) __VA_ARGS__) : printf(format"\n" __VA_OPT__(,)  __VA_ARGS__))
#define log(format, ...) printf("\x1b[35m%s:%d: " format "\x1b[39m\n",  __FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__) 
// get sizeof on compile time for uninitialized structures
#define sizeof_ct(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) {
        printf("  ");
    }
}