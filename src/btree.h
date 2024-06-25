#pragma once


#include "common.h"


uint32_t get_node_max_key(Pager* pager, Node* _node) {
    if (_node->common_header.type == NODE_INTERNAL) {
        InternalNode* node = (InternalNode*)_node;
        Node* last_child = get_page(pager, node->last_child);
        return get_node_max_key(pager, last_child);
    } else {
        LeafNode* node = (LeafNode*)_node;
        return node->cells[node->num_cells - 1].key;
    }
}

void initialize_internal_node(InternalNode* node) {
    // memset(node, 0, PAGE_SIZE);
    // if we are initializing a root node, all of these are pointless
    node->is_root = false;
    node->num_keys = 0;
    node->type = NODE_INTERNAL;
    node->last_child = INVALID_PAGE_NUM;
}

/*
create new parent root node for a current root node that is being split into two.
allocate new page for left node, point both nodes to new root.
we do this instead of allocating a new root and not copying over memory,
so that table->root_page_num can stay the same forever.
*/
void create_new_root(Table* table, uint32_t new_child_page_num) {
    // RESEARCH: could also request a new page and point new root node there, rather than memcpy
    uint32_t old_child_new_page_num = get_unused_page_num(table->pager);

    // may not be an internal node yet, but we'll turn it into one
    InternalNode* root_node = (InternalNode*)get_page(table->pager, table->root_page_num);
    // old child will get assigned to a new page
    Node* old_child_new_node = get_page(table->pager, old_child_new_page_num);
    Node* new_child_node = get_page(table->pager, new_child_page_num);

    if (root_node->type == NODE_INTERNAL) {
        // `new_child_node` is presumably uninitialized
        initialize_internal_node((InternalNode*)new_child_node);
        initialize_internal_node((InternalNode*)old_child_new_node);
    }

    memcpy(old_child_new_node, root_node, PAGE_SIZE);
    old_child_new_node->common_header.is_root = false;
    uint32_t old_child_key = get_node_max_key(table->pager, old_child_new_node);
    old_child_new_node->common_header.parent = table->root_page_num;
    
    new_child_node->common_header.parent = table->root_page_num;

    if (old_child_new_node->common_header.type == NODE_INTERNAL) {
        InternalNode* old_child_node = (InternalNode*)old_child_new_node;
        /* 
        we're splitting an internal node, and haven't moved the last half of children over yet.
        so we can promptly update all of its children's parent pointers before splitting
        */
        Node* sub_child;
        for (uint32_t i = 0; i < old_child_node->num_keys; i++) {
            sub_child = get_page(table->pager, old_child_node->_cells[i].child);
            sub_child->common_header.parent = old_child_new_page_num;
        }
        sub_child = get_page(table->pager, old_child_node->last_child);
        sub_child->common_header.parent = old_child_new_page_num;
    }

    initialize_internal_node(root_node);
    root_node->is_root = true;
    root_node->num_keys = 1;
    root_node->_cells[0].child = old_child_new_page_num;
    root_node->_cells[0].key = old_child_key;
    root_node->last_child = new_child_page_num;

}

/* helper function that returns index of a child where
given key is equal to or greater than the child's key,
but less than the next child's key.  */
static uint32_t internal_node_find_child(
    InternalNode* node,
    uint32_t key
) {
    uint32_t min_index = 0;
    uint32_t max_index = node->num_keys;
    while (min_index < max_index) {
        uint32_t curr_index = (min_index+max_index)/2;
        uint32_t curr_key = node->_cells[curr_index].key;
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
    InternalNode* node = (InternalNode*)get_page(table->pager, node_num);
    uint32_t child_idx = internal_node_find_child(node, key);
    Cursor* cursor = malloc(sizeof *cursor);
    cursor->table = table;
    cursor->page_num = node_num;
    cursor->cell_num = child_idx;
    return cursor;
}

/*
return the page number (not pointer) of a given child index
searches key-cell pairs for an index lower than `num_keys`,
or returns `last_child` for an index equal to `num_keys`
*/
uint32_t* internal_node_child(InternalNode* node, uint32_t child_idx) {
    if (child_idx > node->num_keys) {
        log("tried accessing child %d", child_idx);
        exit(EXIT_FAILURE);
    /* child page number offset is 0,
    so we can just use the cell or last_child pointer directly */
    } else if (child_idx == node->num_keys) {
        return &(node->last_child);
    } else {
        return &(node->_cells[child_idx].child);
    }
}

void update_internal_node_key(
    InternalNode* node,
    uint32_t old_key,
    uint32_t new_key
) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    node->_cells[old_child_index].key = new_key;
}

static void internal_node_split_and_insert(
    Table* table,
    uint32_t old_sibling_page_num,
    uint32_t insert_node_num
);

void internal_node_insert(
    Table* table,
    uint32_t parent_page_num,
    uint32_t insert_page_num
) {
    InternalNode* parent_node = (InternalNode*)get_page(table->pager, parent_page_num);
    Node* insert_node = (Node*)get_page(table->pager, insert_page_num);
    // a node with `last_child == INVALID_PAGE_NUM` is empty
    if (parent_node->last_child == INVALID_PAGE_NUM) {
        parent_node->last_child = insert_page_num;
        return;
    }
    uint32_t insert_node_key = get_node_max_key(table->pager, insert_node);
    uint32_t insert_idx = internal_node_find_child(parent_node, insert_node_key);
    if (parent_node->num_keys >= INTERNAL_NODE_MAX_KEYS) { // we're already at the limit, inserting one more would overflow
        internal_node_split_and_insert(table, parent_page_num, insert_page_num);
        return;
    }
    // if () {
    /* 
    we compare keys rather than `insert_idx >= parent_node->num_keys`
    because `num_keys == 0` guarantees `insert_idx == 0`, 
    however `num_keys` may be 0 but we may already have a `last_child`,
    and `internal_node_find_child` is blind to that possibility.
    */
    if (insert_node_key > get_node_max_key(table->pager, (Node*)parent_node)) {
        // node to be inserted should be the new last child - swap with current last child
        Node* last_child_node = get_page(table->pager, parent_node->last_child);
        parent_node->_cells[parent_node->num_keys].child = parent_node->last_child;
        parent_node->_cells[parent_node->num_keys].key = get_node_max_key(table->pager, last_child_node);
        parent_node->last_child = insert_page_num;
        // RESEARCH: should we update the parent's key on *its* parent?
    } else {
        // [0, 1, 3, 4] [*] (invalid memory)
        //      ^^          ^ parent_num_keys
        for (uint32_t i = parent_node->num_keys; i > insert_idx + 1; i--) {
            memcpy(
                &(parent_node->_cells[i]),
                &(parent_node->_cells[i-1]),
                INTERNAL_NODE_CELL_SIZE
            );
        }
        parent_node->_cells[insert_idx].child = insert_page_num;
        parent_node->_cells[insert_idx].key = get_node_max_key(table->pager, insert_node);
    }
    parent_node->num_keys++;
}

static void internal_node_split_and_insert(
    // TODO: probably wrong signature
    Table* table,
    uint32_t old_sibling_page_num,
    uint32_t insert_node_num
) {
    InternalNode* old_sibling_node = (InternalNode*)get_page(table->pager, old_sibling_page_num);
    uint32_t old_sibling_old_key = get_node_max_key(table->pager, (Node*)old_sibling_node);

    Node* insert_node = (Node*)get_page(table->pager, insert_node_num);
    uint32_t insert_key = get_node_max_key(table->pager, insert_node);

    uint32_t new_sibling_num = get_unused_page_num(table->pager);
    InternalNode* new_sibling_node = (InternalNode*)get_page(table->pager, new_sibling_num);

    // parent of two nodes resulting from split
    InternalNode* parent_node;
    /*
    we branch here because `create_new_root` initializes `new_node_num` for us,
    but if we don't use it, then we have to initialize `new_node` ourselves.
    */
    bool splitting_root = old_sibling_node->is_root;
    if (splitting_root) {
        // parent node is newly created
        create_new_root(table, new_sibling_num);
        parent_node = (InternalNode*)get_page(table->pager, table->root_page_num);
        // since `old_sibling_node` moved to a new page, the current page num and page pointer are invalid - update them
        old_sibling_page_num = parent_node->_cells[0].child;
        old_sibling_node = (InternalNode*)get_page(table->pager, old_sibling_page_num);
    } else {
        parent_node = (InternalNode*)get_page(table->pager, old_sibling_node->parent);
    }
    initialize_internal_node(new_sibling_node);

    uint32_t cur_page_num = old_sibling_node->last_child;
    Node* cur_node = get_page(table->pager, cur_page_num);

    // make `old_sibling_node`'s `last_child`, `new_sibling_node`'s `last_child`
    internal_node_insert(table, new_sibling_num, cur_page_num);
    cur_node->common_header.parent = new_sibling_num;
    old_sibling_node->last_child = INVALID_PAGE_NUM;

    /* 
    all children (except `last_child`) are currently in the old node.
    let's move them one by one and update `num_keys` in the old node accordingly.
    */
    for (uint32_t i = INTERNAL_NODE_MAX_KEYS - 1; i > (INTERNAL_NODE_MAX_KEYS) / 2; i--) {
        cur_page_num = old_sibling_node->_cells[i].child;
        cur_node = get_page(table->pager, cur_page_num);

        internal_node_insert(table, new_sibling_num, cur_page_num);
        cur_node->common_header.parent = new_sibling_num;
        old_sibling_node->num_keys--;
    }

    // make current "last" child (last cell, invalid state) into `last_child`
    old_sibling_node->last_child = old_sibling_node->_cells[old_sibling_node->num_keys-1].child;
    old_sibling_node->num_keys--;
    // since `old_sibling_node` has an updated `last_child`, update its corresponding key in its parent also
    uint32_t old_sibling_new_key = get_node_max_key(table->pager, (Node*)old_sibling_node);
    update_internal_node_key(parent_node, old_sibling_old_key, old_sibling_new_key);

    uint32_t destination_page_num = (insert_key > old_sibling_new_key) ? new_sibling_num : old_sibling_page_num;
    internal_node_insert(table, destination_page_num, insert_node_num);
    insert_node->common_header.parent = destination_page_num;

    if (!splitting_root) {
        // we have to manually insert new sibling, `create_new_root` doesn't do it for us
        internal_node_insert(table, old_sibling_node->parent, new_sibling_num);
        new_sibling_node->parent = old_sibling_node->parent;
    }
}

void initialize_leaf_node(LeafNode* node) {
    // memset(node, 0, PAGE_SIZE);
    // memset 0 already does all these
    node->is_root = false;
    node->num_cells = 0;
    node->next_leaf = 0;
    node->type = NODE_LEAF;
}

/*
find cell with matching key, OR cell ideal for inserting the key.
return cursor may point to cell past limit and node splitting should be handled.
ex: [0, 1, 2, 3,][*]
            limit ^ ^ return_cursor->cell_num
*/
Cursor* leaf_node_find(Table* table, uint32_t node_num, uint32_t key) {
    LeafNode* node = (LeafNode*)get_page(table->pager, node_num);
    Cursor* cursor = malloc(sizeof *cursor);
    cursor->table = table;
    cursor->page_num = node_num;
    // binary search
    uint32_t min_index = 0;
    // keys are monotonic but not necessarily continuons. e.g. [2, 3, 8, 14]
    uint32_t one_past_max_index = node->num_cells;
    while (one_past_max_index != min_index) {
        //`min + (max-min) / 2` simplifies into `(min + max) / 2`
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = node->cells[index].key;
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

/*
create new sibling node,
move over top half of items (rounding down),
while inserting new value into appropriate node
*/
static void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    LeafNode* old_node = (LeafNode*)get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_key = get_node_max_key(cursor->table->pager, (Node*)old_node);

    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    LeafNode* new_node = (LeafNode*)get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    new_node->next_leaf = old_node->next_leaf;
    new_node->num_cells = LEAF_NODE_RIGHT_SPLIT_COUNT;
    new_node->parent = old_node->parent;

    old_node->next_leaf = new_page_num;
    old_node->num_cells = LEAF_NODE_LEFT_SPLIT_COUNT;

    /* because we're also inserting, we have to account for a cell that doesn't exist yet.
    (because max_cells == max_idx + 1) */
    for (uint32_t i = LEAF_NODE_MAX_CELLS; i > 0; i--) {
        /*
        cell_num > insert_cell: move over, copy memory from i-1 to i, possibly to sibling node
        cell_num == insert_cell: equals `i`, copy there.
        cell_num < insert_cell: don't move over, but possibly copy to sibling node.
        */
        uint32_t new_cell_idx = i % LEAF_NODE_RIGHT_SPLIT_COUNT; // new index for cells after insert_cell
        LeafNode* destination_node = (i >= LEAF_NODE_RIGHT_SPLIT_COUNT) ? new_node : old_node;
        void *destination = &(destination_node->cells[new_cell_idx]);
        if (i == cursor->cell_num) {
            destination_node->cells[new_cell_idx].key = key;
            serialize_row(value, destination);
        } else if (i < cursor->cell_num) { // before insert
            // NOTE: if destination_node == old_node, this actually does nothing!
            memcpy(destination, &(old_node->cells[i]), LEAF_NODE_CELL_SIZE);
        } else { // past insert
            memcpy(destination, &(old_node->cells[i-1]), LEAF_NODE_CELL_SIZE);
        }
    }

    if (old_node->is_root) {
        create_new_root(cursor->table, new_page_num);
    } else {
        uint32_t parent_page_num = old_node->parent;
        InternalNode* parent_node = (InternalNode*)get_page(cursor->table->pager, parent_page_num);
        // we haven't inserted the new node yet, so no need to update its key
        uint32_t new_key = get_node_max_key(cursor->table->pager, (Node*)old_node);
        update_internal_node_key(parent_node, old_key, new_key);
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
    }
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    LeafNode* node = (LeafNode*)get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = node->num_cells;
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells) {
        // key is to be inserted between 2 existing cells
        // shift cells to the right to insert in the middle
        for (uint32_t i = num_cells; i>cursor->cell_num; i--) {
            memcpy(&(node->cells[i]), &(node->cells[i-1]), LEAF_NODE_CELL_SIZE);
        }
    // TODO: check whether this code is necessary
    } else if (!node->is_root) {
        // largest key yet - update parent node's key on current node
        // above check is there because a root node has no parent
        uint32_t old_key = get_node_max_key(cursor->table->pager, (Node*)node);
        InternalNode* parent_node = (InternalNode*)get_page(cursor->table->pager, node->parent);
        update_internal_node_key(parent_node, old_key, key);
    }

    node->num_cells += 1;
    // node->cells[cursor->cell_num].key = key; // guaranteed by `serialize_row`
    // serialize row in memory
    serialize_row(value, &(node->cells[cursor->cell_num]));
}

/*
navigate down an internal node `node_num`  until it finds a leaf node with matching `key`,
or cell to insert `key` into, and return a cursor pointing to it.
*/
Cursor* internal_node_find_leaf(Table* table, uint32_t node_num, uint32_t key) {
    InternalNode* node = (InternalNode*)get_page(table->pager, node_num);
    uint32_t child_idx = internal_node_find_child(node, key);
    uint32_t child_page_num = *internal_node_child(node, child_idx);
    Node* child_ptr = (Node*)get_page(table->pager, child_page_num);
    switch (child_ptr->common_header.type) {
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
