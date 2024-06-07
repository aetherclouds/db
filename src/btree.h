#pragma once

#include "common.h"


/* mode common */
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t); // 1 byte bool
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
/* leaf head */
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;
/* leaf body */
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t); // key = row number
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_OFFSET;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
/* leaf util */
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_SPLIT_POS = (LEAF_NODE_MAX_CELLS+1) / 2; // point at which (inclusive) node should split to right sibling
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS+1)-LEAF_NODE_SPLIT_POS; // point at which (inclusive) node should split to right sibling

/* internal head */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
/* we have a separate header for the last child because
if we have N children, we have N-1 keys, that therefore use N-1 cells.
*/
const uint32_t INTERNAL_NODE_LAST_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_LAST_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_LAST_CHILD_SIZE;
/* internal body */
// children[last] < key <= last_child
// child pointer is ordered
const uint32_t INTERNAL_NODE_CHILD_OFFSET = 0;
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_KEY_OFFSET = INTERNAL_NODE_CHILD_SIZE;
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t INTERNAL_NODE_SPACE_FOR_CELLS = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE;
// const uint32_t INTERNAL_NODE_MAX_CELLS = INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;
const uint32_t INTERNAL_NODE_MAX_CHILDREN = INTERNAL_NODE_MAX_CELLS + 1;

NodeType get_node_type(void* node) {
    uint8_t value = *(uint8_t*)(node + NODE_TYPE_OFFSET);
    return (NodeType)value;
}

void set_node_type(void* node, NodeType node_type) {
    uint8_t* value = (uint8_t*)(node + NODE_TYPE_OFFSET);
    *value = (uint8_t)node_type;
}

bool is_node_root(void* node) {
    uint8_t value = *(uint8_t*)(node+IS_ROOT_OFFSET);
    // value > 0?
    return (bool)value;
}

void set_node_root(void* node, bool value) {
    *(uint8_t*)(node+IS_ROOT_OFFSET) = (uint8_t)value;
}

uint32_t* node_parent(void* node) {
    return node + PARENT_POINTER_OFFSET;
}

uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t* leaf_node_next_leaf(void*node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void*node, uint32_t cell_num) {
    return (uint32_t*)(leaf_node_cell(node, cell_num));
}


void* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_VALUE_OFFSET;
}

uint32_t* internal_node_num_keys(void* node) {
    return (uint32_t*)(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t* internal_node_last_child(void* node) {
    return (uint32_t*)(node + INTERNAL_NODE_LAST_CHILD_OFFSET);
}

void* internal_node_cell(void* node, uint32_t cell_num) {
    return (void*)(node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t* internal_node_key(void* node, uint32_t cell_num) {
    return internal_node_cell(node, cell_num) + INTERNAL_NODE_KEY_OFFSET;
}

void initialize_leaf_node(void* node) {
    memset(node, 0, PAGE_SIZE);
    // memset 0 already does all these
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;
    set_node_root(node, false);
    set_node_type(node, NODE_LEAF);
}

void initialize_internal_node(void* node) {
    memset(node, 0, PAGE_SIZE);
    // if we are initializing a root node, unfortunately all of these are pointless
    *internal_node_num_keys(node) = 0;
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
}

uint32_t get_node_max_key(void* node) {
    switch (get_node_type(node)) {
    case NODE_LEAF:
        return *leaf_node_key(node, *leaf_node_num_cells(node)-1);
    case NODE_INTERNAL:
        return *internal_node_key(node, *internal_node_num_keys(node)-1);
    default:
        break;
    }
}

Cursor* leaf_node_find(Table* table, uint32_t node_num, uint32_t key) {
    /*
    find cell with matching key, OR cell ideal for inserting the key.
    return cursor may point to cell past limit and node splitting should be handled.
    ex: [0, 1, 2, 3,][*]
              limit ^ ^ return_cursor->cell_num
    */
    void* node = get_page(table->pager, node_num);
    // keys are monotonci but not necessarily continuons. e.g. [2, 3, 8, 14]
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = node_num;
    // binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index) {
        //`min + (max-min) / 2` simplifies into `(min + max) / 2`
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index) {
            cursor->cell_num = index;
            return cursor;
        }
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            // we already checked this index, so increment it.
            // if we have found a place to insert a unique key, this is where it is
            min_index = index+1;
        }
    }

    // num_cells == 0, or key is not found but now we have a position to insert it into.
    cursor->cell_num = min_index;
    return cursor;
}

static uint32_t internal_node_find_child(
    void* node,
    uint32_t key
) {
    /* helper function that returns index of a child where
     given key is equal to or greater than the child's key,
    but less than the next child's key.  */
    uint32_t min_index = 0;
    uint32_t max_index = *internal_node_num_keys(node);
    while (min_index < max_index) {
        uint32_t curr_index = (min_index+max_index)/2;
        uint32_t curr_key = *internal_node_key(node, curr_index);
        if (key > curr_key) {
            min_index = curr_index + 1;
        } else if (key < curr_key) {
            max_index = curr_index;
        } else {
            min_index = curr_index;
            break;
        }
    }
    return min_index;
}

Cursor* internal_node_find(
    Table* table,
    uint32_t node_num,
    uint32_t key
) {
    void* node = get_page(table->pager, node_num);
    uint32_t child_idx = internal_node_find_child(node, key);
    Cursor* cursor = malloc(sizeof(cursor));
    cursor->table = table;
    cursor->page_num = node_num;
    cursor->cell_num = child_idx;
    return cursor;
}

uint32_t* internal_node_child(void* node, uint32_t child_idx) {
    /*
    return the page number (not pointer) of a given child index
    searches key-cell pairs for an index lower than `num_keys`,
    or returns `last_child` for an index equal to `num_keys`
     */
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_idx > num_keys) {
        log("tried accessing child %d", child_idx);
        exit(EXIT_FAILURE);
    /* child page number offset is 0,
    so we can just use the cell or last_child pointer directly */
    } else if (child_idx == num_keys) {
        return internal_node_last_child(node);
    } else {
        return internal_node_cell(node, child_idx) + INTERNAL_NODE_CHILD_OFFSET;
    }
}

Cursor* internal_node_find_leaf(Table* table, uint32_t node_num, uint32_t key) {
    void* node = get_page(table->pager, node_num);
    uint32_t child_idx = internal_node_find_child(node, key);
    uint32_t child_page_num = *internal_node_child(node, child_idx);
    void* child_ptr = get_page(table->pager, child_page_num);
    switch (get_node_type(child_ptr)) {
    case NODE_INTERNAL:
        return internal_node_find_leaf(table, child_page_num, key);
    case NODE_LEAF:
        return leaf_node_find(table, child_page_num, key);
    default:
        log("invalid node type");
        exit(EXIT_FAILURE);
        break;
    }
}

void update_internal_node_key(
    void* node,
    uint32_t old_key,
    uint32_t new_key
) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *(internal_node_key(node, old_child_index)) = new_key;
}

void create_new_root(Table* table, uint32_t right_child_page_num) {
    /*
    create new parent node for a previously root node that is being split into two.
    allocate new page for left node, point both nodes to new root.
    we do this instead of allocating a new root and not copying over memory,
    so that table->root_page_num can stay the same forever.
    */
    void* root_node = get_page(table->pager, table->root_page_num);

    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void* left_child_node = get_page(table->pager, left_child_page_num);
    void* right_child_node = get_page(table->pager, right_child_page_num);
    memcpy(left_child_node, root_node, PAGE_SIZE);

    initialize_internal_node(root_node);
    set_node_root(root_node, true);
    set_node_root(left_child_node, false);
    *internal_node_num_keys(root_node) = 1;
    uint32_t left_child_key = get_node_max_key(left_child_node);
    *internal_node_key(root_node, 0) = left_child_key;
    *internal_node_child(root_node, 0) = left_child_page_num;
    *internal_node_last_child(root_node) = right_child_page_num;

    *node_parent(left_child_node) = table->root_page_num;
    *node_parent(right_child_node) = table->root_page_num;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-W"
void internal_node_insert(
    Table* table,
    uint32_t parent_page_num,
    uint32_t child_page_num
) {
    // get position of old cell
    void* parent_node = get_page(table->pager, parent_page_num);
    void* child_node = get_page(table->pager, child_page_num);
    uint32_t parent_num_keys = *internal_node_num_keys(parent_node);
    uint32_t child_node_key = get_node_max_key(child_node);
    uint32_t left_sibling_idx = internal_node_find_child(parent_node, child_node_key);
    uint32_t insert_idx = left_sibling_idx + 1;
    (*internal_node_num_keys(parent_node))++;
    if (parent_num_keys >= INTERNAL_NODE_MAX_CELLS) { // we're already at the limit, inserting one more would overflow
        log("TODO: split current node");
        exit(EXIT_FAILURE);
    }
    if (insert_idx >= INTERNAL_NODE_MAX_CELLS) {
        // node to be inserted should be the new last child - swap with current last child
        uint32_t* last_child_page_num_ptr = internal_node_last_child(parent_node);
        uint32_t last_child_page_num = *last_child_page_num_ptr;
        void* last_child = get_page(table->pager, last_child_page_num);

        *last_child_page_num_ptr = child_page_num;


    } else {
        // [0, 1, 3, 4] [*] (invalid memory)
        //      ^^          ^ parent_num_keys
        for (uint32_t i = parent_num_keys - 1; i > insert_idx + 1; i--) {
            memcpy(
                internal_node_cell(parent_node, i),
                internal_node_cell(parent_node, i-1),
                INTERNAL_NODE_CELL_SIZE
            );
        }
        *internal_node_child(parent_node, insert_idx) = child_page_num;
        *internal_node_key(parent_node, parent_num_keys) = get_node_max_key(child_node);
    }
}

static void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    /*
    create new sibling node,
    move over top half of items (rounding down),
    while inserting new value into appropriate node
    */
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_key = get_node_max_key(old_node);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);

    *leaf_node_next_leaf(old_node) = new_page_num;
    *leaf_node_num_cells(old_node) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *leaf_node_num_cells(new_node) = LEAF_NODE_SPLIT_POS;
    *node_parent(new_node) = *node_parent(old_node);

    /* because we're also inserting, we have to account for a cell that doesn't exist yet.
    (because max_cells == max_idx + 1) */
    for (uint32_t i = LEAF_NODE_MAX_CELLS; i > 0; i--) {
        /*
        cell_num > insert_cell: move over, copy memory from i-1 to i, possibly to sibling node
        cell_num == insert_cell: equals `i`, copy there.
        cell_num < insert_cell: don't move over, but possibly copy to sibling node.
        */
        uint32_t new_cell_idx = i % LEAF_NODE_SPLIT_POS; // new index for cells after insert_cell
        void *destination_node = (i >= LEAF_NODE_SPLIT_POS) ? new_node : old_node;
        void *destination = leaf_node_cell(destination_node, new_cell_idx);
        if (i == cursor->cell_num) {
            *leaf_node_key(destination_node, new_cell_idx) = key;
            serialize_row(value, destination + LEAF_NODE_VALUE_OFFSET);
        } else if (i < cursor->cell_num) { // before insert
            // NOTE: if destination_node == old_node, this actually does nothing!
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        } else { // past insert
            memcpy(destination, leaf_node_cell(old_node, i-1), LEAF_NODE_CELL_SIZE);
        }
    }

    if (is_node_root(old_node)) {
        create_new_root(cursor->table, new_page_num);
    } else {
        log("TODO: update internal node after split");
        exit(EXIT_FAILURE);
        uint32_t parent_page_num = *node_parent(old_node);
        void* parent_node = get_page(cursor->table->pager, parent_page_num);
        // reminder that key of node N is first item of node N+1
        // uint32_t new_key = *leaf_node_key(new_node, 0);
        uint32_t new_key = get_node_max_key(old_node);
        update_internal_node_key(parent_node, old_key, new_key);
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
    }
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells) {
        // shift cells to the right to insert in the middle
        for (uint32_t i = num_cells; i>cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i-1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    // serialize row in memory
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}
