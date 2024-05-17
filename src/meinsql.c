#include "common.h"
#include "btree.h"


typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
} PrepareResult;

typedef enum {
    EXECUTE_TABLE_FULL,
    EXECUTE_SUCCESS,
    EXECUTE_FAILURE,
    EXECUTE_DUPLICATE_KEY,
} ExecuteResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    char* buffer;
    // TODO: understand why this is larger than input
    size_t buffer_length; // size of buffer allocated; buffer_length >= input_length
    ssize_t input_length; // signed because that's getline's return type
} InputBuffer;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

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


bool use_color = true;

void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        print_error("tried to fetch a page number larger than max. allowed: %d > %d", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if (pager->pages[page_num] == NULL) {
        // cache miss; load or create new page
        void* page = calloc(1, PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        // there may be an extra, partial page
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        // we're requesting a page that's within num_pages, so it must exist on disk but not in memory. load it.
        // (reminder that pages are 0 indexed so we check for equality as well)
        if (page_num < num_pages) {
            uint32_t offset = page_num * ROWS_PER_PAGE * ROW_SIZE;
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

// use a pager and page_num instead of a cursor because it's recursive - we'd have to copy cursor for each level
void print_tree(Pager* pager, uint32_t page_num, uint32_t indent_level) {
    void* node = get_page(pager, page_num);

    // indent(indent_level);
    printf("page %d; ", page_num);
    if (is_node_root(node)) {
        printf("root; ");
    }
    switch (get_node_type(node)) {
    case NODE_INTERAL:
        uint32_t num_keys = *leaf_node_num_cells(node);
        printf("internal; %d keys\n", num_keys);
        // N keys == N+1 children
        for (uint32_t i = 0; i < num_keys; i++) {
            indent(indent_level+1);
            uint32_t child_page_num = *internal_node_child(node, i);
            uint32_t child_key = *internal_node_key(node, i);
            printf("+ key %d; ", child_key);
            print_tree(pager, child_page_num, indent_level+1);
        }
        indent(indent_level+1);
        printf("+ ");
        uint32_t last_child_page_num = *internal_node_last_child(node);
        print_tree(pager, last_child_page_num, indent_level+1);
        break;
    case NODE_LEAF:
        uint32_t num_cells = *leaf_node_num_cells(node);
        printf("leaf; %d keys\n", num_cells);
        for (uint32_t i = 0; i < num_cells; i++) {
            indent(indent_level+1);
            printf("- key %d\n", *leaf_node_key(node, i));
        }
        break;
    }
}

Pager* pager_open(const char* filename) {
    int fd = open(filename,
            O_RDWR | O_CREAT,
            S_IWUSR | S_IRUSR
    );
    if (fd == -1) {
        print_error("file could not be opened: %s", filename);
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;
    if (file_length % PAGE_SIZE != 0) {
        print_error("database file is not a whole number of pages - corrupt file");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL; // initially, no pages are loaded
    }
    return pager;
}

void pager_flush(Pager* pager, uint32_t page_num) {
    if (pager->pages[page_num] == NULL) {
        // program may follow this branch if flushing a page that hasn't been loaded to cache yet, but is stored no disk
        // (this happens because we measure how many pages to flush based on filesize, not )
        print_error("tried to flush null page: %d", page_num);
        // exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        print_error("error seeking; %d", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        print_error("failed writing to file: %d", errno);
        exit(EXIT_FAILURE);
    }
}

Cursor* table_start(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;
    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);
    return cursor;
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

Cursor* table_find(Table* table, uint32_t key) {
    /* 
    returns a cursor pointing to a cell with matching key,
    or if key wasn't found, the cell we could insert into.
    */
    uint32_t root_page_num = table->root_page_num;
    // page == node
    void* root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    } else {
        log("TODO:");
        exit(EXIT_FAILURE);
    }
}

Cursor* table_end(Table* table) {
    // remember that `table_end` takes us to one step *past* the end of the table (where it's safe to insert).
    // this means that, for example, if current page has a limit number of cells, we'll be at limit+1
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->cell_num = num_cells;
    cursor->end_of_table = true;

    return cursor;
}
Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);

    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;

    if (pager->num_pages == 0) {
        // new file - initialize page 0 as leaf node
        void* root_node = get_page(table->pager, 0);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
    }
    return table;
}

void db_close(Table* table) {
    Pager* pager = table->pager;
    // first, we flush full pages, then a partial page.
    for (uint32_t i = 0; i < pager->num_pages; i++) {
        if (pager->pages[i] == NULL) continue;

        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        print_error("failed to close db file");
        exit(EXIT_FAILURE);
    }
    // I don't get why we free all pages *again* on this part of the tutorial, so I'll just ignore it
    // ...
    free(pager);
    free(table);
}

void* cursor_value(Cursor* cursor){
    // get pointer to current row, create new page if needed.
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);
    cursor->cell_num += 1;

    if (cursor->cell_num >= *leaf_node_num_cells(node)) {
        cursor->end_of_table = true;
    }
}

void print_row(Row* row){
    // printf("%*d %*s %*s\n", 2, row->id, 32, row->username, 25, row->email);
    printf("%d %s %s\n",row->id, row->username, row->email);
    // TODO: WHY DOES THIS WORK?? // printf("%*d %*s %*s\n", row->id,  row->username, 25, row->email, 255);
}

void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void print_prompt() { printf("db > ");}

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

uint32_t get_unused_page_num(Pager* pager) {
    // append new page to end of database file for now
    return pager->num_pages;
}

void create_new_root(Table* table, uint32_t right_child_page_num) {
    /*
    create new parent node for (presumably) a root node that was split into two.
    allocate new page for left node, point both nodes to new root.
    we do this instead of allocating a new root and not copying over memory,
    so that table->root_page_num can stay the same forever.
    */
    void* root_node = get_page(table->pager, table->root_page_num);

    void* right_child_node = get_page(table->pager, right_child_page_num);

    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void* left_child_node = get_page(table->pager, left_child_page_num);
    memcpy(left_child_node, root_node, PAGE_SIZE);

    initialize_internal_node(root_node);
    set_node_root(root_node, true);
    set_node_root(left_child_node, false);
    *internal_node_num_keys(root_node) = 1;
    // uint32_t left_child_last_key = get_node_last_key(left_child_node);
    // FIXME: code won't work for internal nodes here
    uint32_t right_child_first_key = *leaf_node_key(right_child_node, 0);
    *internal_node_key(root_node, 0) = right_child_first_key;
    *internal_node_child(root_node, 0) = left_child_page_num;
    *internal_node_last_child(root_node) = right_child_page_num;

    *node_parent(left_child_node) = table->root_page_num;
    *node_parent(right_child_node) = table->root_page_num;
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    /*
    create new sibling node, move over top half of items rounding down)
    and insert new value in adequate node(
    */
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);

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
            serialize_row(value, destination);
            *leaf_node_key(destination_node, new_cell_idx) = key;
        } else if (i < cursor -> cell_num) { // before insert
            // NOTE: if destination_node == old_node, this actually does nothing!
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        } else { // past insert
            memcpy(destination, leaf_node_cell(old_node, i-1), LEAF_NODE_CELL_SIZE);
        }
    }
    
    *leaf_node_num_cells(old_node) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *leaf_node_num_cells(new_node) = LEAF_NODE_SPLIT_POS; 

    if (is_node_root(old_node)) {
        return create_new_root(cursor->table, new_page_num);
    } else {
        log("TODO: update internal node after split");
        exit(EXIT_FAILURE);
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

// copying a structure takes a lot more CPU time than copying a pointer - https://softwareengineering.stackexchange.com/a/359410
InputBuffer* new_input_buffer(){
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void close_input_buffer(InputBuffer* input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

void read_input(InputBuffer* input_buffer){
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        print_error("%s", "error reading input");
        exit(EXIT_FAILURE);
    }

    // remove newline
    // we can concat string by setting a new null termnator
    input_buffer->input_length = bytes_read-1;
    input_buffer->buffer[bytes_read-1] = '\0';
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    // only using `strncmp` rather than `strcmp` here because I don't like that the commands won't execute if there is a space at the end. that's all.
    if (strncmp(input_buffer->buffer, ".exit", 5) == 0) {
        print_success("%s", "exiting");
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
        return META_COMMAND_SUCCESS;
    } else if (strncmp(input_buffer->buffer, ".print", 6) == 0) {
        printf("constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    } else if (strncmp(input_buffer->buffer, ".btree", 6) == 0) {
        print_tree(table->pager, table->root_page_num, 0);
        return META_COMMAND_SUCCESS;
    }

    return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    // int n_args_assigned = sscanf(input_buffer->buffer, "insert %d %33s %256s", &(statement->row_to_insert.id), &(statement->row_to_insert.username), &(statement->row_to_insert.email));
        statement->type = STATEMENT_INSERT;

        strtok(input_buffer->buffer, " ");
        char* id_string = strtok(NULL, " ");
        char* username = strtok(NULL, " ");
        char* email = strtok(NULL, " ");

        if (id_string == NULL || username == NULL || email == NULL) {
            return PREPARE_SYNTAX_ERROR;
        }

        int id = atoi(id_string);
        if (strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE) {
            return PREPARE_STRING_TOO_LONG;
        }

        statement->row_to_insert.id = id;
        strcpy(statement->row_to_insert.username, username);
        strcpy(statement->row_to_insert.email, email);

        return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return prepare_insert(input_buffer, statement);
    }
    if (strncmp(input_buffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
ExecuteResult execute_select(Statement* statement, Table* table){
#pragma GCC diagnostic pop
    // NOTE: selecting all rows, for now
    Row row;
    Cursor* cursor = table_start(table);
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert(Statement* statement, Table* table){
    void* node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Row* row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor* cursor = table_find(table, key_to_insert);

    if (cursor->cell_num < num_cells) { // inserting between
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_insert) return EXECUTE_DUPLICATE_KEY;
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table){
    switch (statement->type){
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
            break;
        case STATEMENT_SELECT:
            return execute_select(statement, table);
            break;
        default:
            log("no case match");
            exit(EXIT_FAILURE);
    };
    // gcc complains about this because https://stackoverflow.com/a/33607345
}


int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_error("must provide a database filename");
        exit(EXIT_FAILURE);
    }
    struct option options[] = {
        {"no-color", no_argument, (int*)&use_color, false},
        {0, 0, 0, 0}
    };
    int opt_idx = 0;
    getopt_long(argc-1, &argv[1], "", options, &opt_idx);

    char* filename = argv[1];
    Table* table = db_open(filename);
    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer); // read into the buffer

        // we'll handle meta-commands (`.`) separately, so `continue` after processing
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    // char* aa = sprintf("%d", 2);
                    print_error("unrecognized meta-command: %s", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                print_error("unrecognized command: %s", input_buffer->buffer);
                continue;
            case PREPARE_SYNTAX_ERROR:
                print_error("incorrect syntax for valid command: %s", input_buffer->buffer);
                continue;
            case PREPARE_STRING_TOO_LONG:
                print_error("string too long for command: %s", input_buffer->buffer);
                continue;
            default:
                print_error("undocumented prepare error");
                continue;
        }

        switch (execute_statement(&statement, table)){
            case EXECUTE_SUCCESS:
                print_success("executed");
                break;
            case EXECUTE_FAILURE:
                print_error("failed to execute statement: undocumented");
                continue;
            case EXECUTE_TABLE_FULL:
                print_error("failed to execute statement: table is full");
                continue;
            case EXECUTE_DUPLICATE_KEY:
                print_error("failed to execute statement: duplicate key: %d", statement.row_to_insert.id);
                continue;
        }
    }

    return 1;
}

