#pragma once


#include <stdint.h>


// get sizeof on compile time for uninitialized structures
#define sizeof_ct(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100


typedef struct {
    // "_t" ensures cross-platform compatibility
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1];
    char email[COLUMN_EMAIL_SIZE+1];
} Row;

typedef enum { NODE_INTERAL, NODE_LEAF } NodeType;


const uint32_t ID_SIZE = sizeof_ct(Row, id);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_SIZE = sizeof_ct(Row, username);
LEAF_NODE_CELL_SIZEconst uint32_t USERNAME_OFFSET = ID_SIZE;
const uint32_t EMAIL_SIZE = sizeof_ct(Row, email);
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; // we do this rather than sizeof(Row), because the latter has bit padding.

// constexpr uint32_t PAGE_SIZE = sysconf(_SC_PAGESIZE);
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t); // 1 byte bool
const uint32_t NODE_TYPE_OFFSET = sizeof(uint8_t);
const uint32_t NODE_IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t NODE_IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = NODE_IS_ROOT_OFFSET + NODE_TYPE_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + NODE_IS_ROOT_SIZE + PARENT_POINTER_SIZE;
/* leaf head */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;
/* leaf body*/
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t); // key = row number
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_OFFSET;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void*node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}


uint32_t* leaf_node_value(void*node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_VALUE_OFFSET;
}

void initialize_leaf_node(void* node) { *leaf_node_num_cells(node) = 0; }

