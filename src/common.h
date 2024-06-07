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

#define COLUMN_USERNAME_SIZE 31
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100


const uint32_t ID_SIZE = sizeof_ct(Row, id);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_SIZE = sizeof_ct(Row, username);
const uint32_t USERNAME_OFFSET = ID_SIZE;
const uint32_t EMAIL_SIZE = sizeof_ct(Row, email);
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; // serialized size

// constexpr uint32_t PAGE_SIZE = sysconf(_SC_PAGESIZE);
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;


typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES]; /* NOTE: NEVER use this outside of code dealing stricly with
    loading pages.*/
} Pager;

typedef struct {
    uint32_t root_page_num;
    Pager* pager;
} Table;

typedef struct {
    /* table, page num and cell_num together
    uniquely identify a cell in a B+ tree node in some table. */
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
} Cursor;


typedef struct {
    // "_t" ensures cross-platform compatibility
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1]; // +1 for C-style strings
    char email[COLUMN_EMAIL_SIZE+1];
} Row;


void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) {
        printf("  ");
    }
}

void serialize_row(Row* source, void* destination) {
    // NOTE: could also use strncpy to initialize bits to 0, looks cleaner on xxd
    memcpy(destination + ID_OFFSET,         &(source->id),      ID_SIZE);
    memcpy(destination + USERNAME_OFFSET,   &(source->username),USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET,      &(source->email),   EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id),       source + ID_OFFSET,        ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET,  USERNAME_SIZE);
    memcpy(&(destination->email),    source + EMAIL_OFFSET,     EMAIL_SIZE);
}

void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        log("tried to fetch a page number larger than max. allowed: %d > %d", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if (pager->pages[page_num] == NULL) {
        // cache miss; load or create new page
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        // there may be an extra, partial page
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        // we're requesting a page that's within num_pages, so it must exist on disk but not in memory. load it.
        // (reminder that pages are 0 indexed so we check for equality as well)
        if (page_num < num_pages) {
            uint32_t offset = page_num * PAGE_SIZE;
            lseek(pager->file_descriptor, offset, SEEK_SET);
            // we don't have to check if it crosses file size - implementation should set off-bounds bytes to 0
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("error reading file: %d", errno);
                exit(EXIT_FAILURE);
            }
        }
        // are we creating a new page? if so, increment page count
        // we can't use `num_pages` here because it's reliant on filesize. we may not have flushed existing new pages yet.
        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}
