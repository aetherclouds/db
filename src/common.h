#pragma once

#define _POSIX_C_SOURCE 200809L
#define NDEBUG

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

#include <assert.h>


// NOTE: requires c2x standard
// `__VA_OPT__` gets replaced with its argument if variadic arguments (e.g. `...`) are present
#define print_success(format, ...) (use_color ? printf("\x1b[32m" format "\x1b[39m\n" __VA_OPT__(,) __VA_ARGS__) : printf(format"\n" __VA_OPT__(,) __VA_ARGS__))
#define print_error(format, ...) (use_color ? printf("\x1b[31m" format "\x1b[39m\n" __VA_OPT__(,) __VA_ARGS__) : printf(format"\n" __VA_OPT__(,)  __VA_ARGS__))
#define log(format, ...) (\
    use_color\
    ? printf("\x1b[35m%s:%d: " format "\x1b[39m\n",  __FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__)\
    : printf("%s:%d: " format "\n",  __FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__)\
)
// get sizeof on compile time for uninitialized structures
#define sizeof_ct(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

#define COLUMN_USERNAME_SIZE 31
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1]; // +1 for C-style strings
    char email[COLUMN_EMAIL_SIZE+1];
} Row;

constexpr const uint32_t ID_SIZE = sizeof_ct(Row, id);
constexpr const uint32_t ID_OFFSET = 0;
constexpr const uint32_t USERNAME_SIZE = sizeof_ct(Row, username);
constexpr const uint32_t USERNAME_OFFSET = ID_SIZE;
constexpr const uint32_t EMAIL_SIZE = sizeof_ct(Row, email);
constexpr const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
constexpr const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; // serialized size
// constexpr const uint32_t PAGE_SIZE = sysconf(_SC_PAGESIZE);
constexpr const uint32_t PAGE_SIZE = 4096; // I think a more relevant name would be `BLOCK_SIZE`
constexpr const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
constexpr const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
const uint32_t INVALID_PAGE_NUM = UINT32_MAX;

typedef union {
    uint32_t key;
    char data[ROW_SIZE];
} SerializedRow;

typedef struct _InternalNode InternalNode;
typedef struct _LeafNode LeafNode;
typedef union _Node Node;


typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;
// RESEARCH: maybe we can fit the same n. of cells without packing these (since there's wasted space)?
// RESEARCH: so far, only INTERNAL_NODE_MAX_KEYS goes 510 -> 509
// typedef enum __attribute__((packed)) { NODE_INTERNAL, NODE_LEAF } NodeType;
// typedef struct __attribute__((packed)) {
typedef struct {
    uint8_t is_root;
    NodeType type;
    uint32_t parent;
} CommonHeader;

constexpr const uint32_t COMMON_NODE_HEADER_SIZE = sizeof(CommonHeader);

typedef struct {
    CommonHeader;
    uint32_t num_keys;
    uint32_t last_child;
} InternalHeader;

typedef struct {
    uint32_t child; // page number
    uint32_t key;
} InternalCell;

constexpr const uint32_t INTERNAL_NODE_CELL_SIZE = sizeof(InternalCell);
constexpr const uint32_t INTERNAL_NODE_SPACE_FOR_CELLS = PAGE_SIZE - sizeof(InternalHeader);
constexpr const uint32_t INTERNAL_NODE_MAX_KEYS = INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE;
// constexpr const uint32_t INTERNAL_NODE_MAX_KEYS = 3;
constexpr const uint32_t INTERNAL_NODE_MAX_CHILDREN = INTERNAL_NODE_MAX_KEYS + 1;

struct _InternalNode {
    InternalHeader;
    /* NOTE: NEVER ACCESS THIS DIRECTLY, use `internal_node_child` */
    InternalCell _cells[INTERNAL_NODE_MAX_KEYS];
};

typedef struct {
    CommonHeader;
    uint32_t num_cells;
    uint32_t next_leaf;
} LeafHeader;

typedef SerializedRow LeafCell;


constexpr const uint32_t LEAF_NODE_CELL_SIZE = sizeof(LeafCell);
constexpr const uint32_t LEAF_NODE_HEADER_SIZE = sizeof(LeafHeader);
constexpr const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
constexpr const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS+1) / 2; // point at which (inclusive) node should split to right sibling
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS+1)-LEAF_NODE_RIGHT_SPLIT_COUNT; // point at which (inclusive) node should split to right sibling


struct  _LeafNode {
    LeafHeader;
    LeafCell cells[LEAF_NODE_MAX_CELLS];
};

union  _Node {
    CommonHeader common_header;
    char data[PAGE_SIZE];
};

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    Node* pages[TABLE_MAX_PAGES]; /* NOTE: NEVER use this outside of code dealing stricly with loading pages.*/
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


bool use_color = true;


void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) {
        printf("  ");
    }
}

void serialize_row(Row* source, SerializedRow* destination) {
    // RESEARCH: could also use strncpy to initialize bits to 0, looks cleaner on xxd
    /*
    NOTE: pointer is cast to void to *a*void (get it?) unwanted type pointer sizing,
    `(void*)` guarantees we are working with byte-sized offsets
    */
    memcpy((void*)destination + ID_OFFSET,         &(source->id),      ID_SIZE);
    memcpy((void*)destination + USERNAME_OFFSET,   &(source->username),USERNAME_SIZE);
    memcpy((void*)destination + EMAIL_OFFSET,      &(source->email),   EMAIL_SIZE);
}

void deserialize_row(SerializedRow* source, Row* destination) {
    memcpy(&(destination->id),      (void*)source + ID_OFFSET,        ID_SIZE);
    memcpy(&(destination->username), (void*)source + USERNAME_OFFSET,  USERNAME_SIZE);
    memcpy(&(destination->email),    (void*)source + EMAIL_OFFSET,     EMAIL_SIZE);
}

Node* get_page(Pager* pager, uint32_t page_num) {
    if (page_num >= TABLE_MAX_PAGES) {
        log("tried to fetch a page number larger than max. allowed: %d > %d", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if (pager->pages[page_num] == NULL) {
        // cache miss; load or create new page
        void* page = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
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

uint32_t get_unused_page_num(Pager* pager) {
    // append new page to end of database file for now
    return pager->num_pages;
}

