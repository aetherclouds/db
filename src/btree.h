#pragma once

#include "common.h"

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100

typedef struct {
    // "_t" ensures cross-platform compatibility
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1]; // +1 for C-style strings
    char email[COLUMN_EMAIL_SIZE+1];
} Row;

typedef enum { NODE_INTERAL, NODE_LEAF } NodeType;


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

/* mode common */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t); // 1 byte bool
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
/* leaf head */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;
/* leaf body */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t); // key = row number
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_OFFSET;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
/* leaf util */
const uint32_t LEAF_NODE_SPLIT_POS = (LEAF_NODE_MAX_CELLS+1) / 2; // point at which (inclusive) node should split to right sibling
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS+1)-LEAF_NODE_SPLIT_POS; // point at which (inclusive) node should split to right sibling
/* internal head */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
/* we have a separate header for the last child because 
if we have N children, we have N-1 keys, and therefore N-1 cells.
*/
const uint32_t INTERNAL_NODE_LAST_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_LAST_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_LAST_CHILD_SIZE;
/* internal body */
// left child[last] < key <= right_child[0]
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + LEAF_NODE_KEY_SIZE;


NodeType get_node_type(void* node) {
    uint8_t value = *(uint8_t*)(node + NODE_TYPE_OFFSET);
    return (NodeType)value;
}

// void** node_parent(void* node) {
uint32_t* node_parent(void* node) {
    return node + PARENT_POINTER_OFFSET;
}

bool is_node_root(void* node) {
    uint8_t value = *(uint8_t*)(node+IS_ROOT_OFFSET);
    // return value > 0 ? true : false;
    return (bool)value;
}

void set_node_root(void* node, bool value) {
    *(uint8_t*)(node+IS_ROOT_OFFSET) = (uint8_t)value;
}

uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void*node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void*node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_VALUE_OFFSET;
}

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *(uint8_t*)(node + NODE_TYPE_OFFSET) = value;
}

void initialize_leaf_node(void* node) {
    memset(node, 0, PAGE_SIZE);
    // *leaf_node_num_cells(node) = 0;
    set_node_type(node, NODE_LEAF);
    // set_node_root(node, false);
}


uint32_t* internal_node_num_keys(void* node) {
    return (uint32_t*)(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

void initialize_internal_node(void* node) {
    memset(node, 0, PAGE_SIZE);
    // if we are initializing a root node, unfortunately all of these are pointless
    *internal_node_num_keys(node) = 0;
    set_node_type(node, NODE_INTERAL);
    set_node_root(node, false);
}

uint32_t* internal_node_last_child(void* node) {
    return (uint32_t*)(node + INTERNAL_NODE_LAST_CHILD_OFFSET);
}

void* internal_node_cell(void* node, uint32_t cell_num) {
    return (void*)(node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t* internal_node_key(void* node, uint32_t cell_num) {
    return internal_node_cell(node, cell_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t cell_num) {
    uint32_t num_keys = *internal_node_num_keys(node);
    if (cell_num > num_keys) {
        log("tried accessing child %d", cell_num);
        exit(EXIT_FAILURE);
    } else if (cell_num == num_keys) {
        return internal_node_last_child(node);
    } else {
        return internal_node_cell(node, cell_num);
    }
}
