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
    StatementType type;
    Row row_to_insert;
} Statement;

typedef struct {
    char* buffer;
    // TODO: understand why this is larger than input
    size_t buffer_length; // size of buffer allocated; buffer_length >= input_length
    ssize_t input_length; // signed because that's getline's return type
} InputBuffer;


bool use_color = true;


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

uint32_t get_unused_page_num(Pager* pager) {
    // append new page to end of database file for now
    return pager->num_pages;
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
        return internal_node_find_leaf(table, root_page_num, key);
        log("TODO: implement internal search");
        exit(EXIT_FAILURE);
    }
}

Cursor* table_start(Table* table) {
    Cursor* cursor = table_find(table, 0);
    // NOTE: should probably place this code inside leaf_node_find
    void* first_node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(first_node);
    cursor->end_of_table = (num_cells == 0);
    return cursor;
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
        uint32_t next_page = *leaf_node_next_leaf(node);
        /* FIXME: either mark page 0 as root always or find way to check if
        value is root node (we don't care) vs actually uninitialized (we care about this), meaning end of table. */
        if (next_page) {
            cursor->page_num = next_page;
            cursor->cell_num = 0;
        } else {
            cursor->end_of_table = true;
        }
    }
}

void print_row(Row* row){
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

// use a pager and page_num instead of a cursor because it's recursive - we'd have to copy cursor for each level
void print_tree(Pager* pager, uint32_t page_num, uint32_t indent_level) {
    void* node = get_page(pager, page_num);

    // indent(indent_level);
    printf("page %d; ", page_num);
    if (is_node_root(node)) {
        printf("root; ");
    }
    uint32_t num_items;
    switch (get_node_type(node)) {
    case NODE_INTERNAL:
        num_items = *leaf_node_num_cells(node);
        printf("internal; %d keys\n", num_items);
        // N keys == N+1 children
        for (uint32_t i = 0; i < num_items; i++) {
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
        num_items = *leaf_node_num_cells(node);
        printf("leaf; %d keys\n", num_items);
        for (uint32_t i = 0; i < num_items; i++) {
            indent(indent_level+1);
            printf("- key %d\n", *leaf_node_key(node, i));
        }
        break;
    }
}

void print_prompt() { printf("db > ");}

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
    Row* row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor* cursor = table_find(table, key_to_insert);

    void* node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

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

