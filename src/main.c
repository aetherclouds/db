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
    size_t buffer_length; // size of buffer allocated; buffer_length >= input_length
    ssize_t input_length; // signed because that's getline's return type
} InputBuffer;


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
    Pager* pager = malloc(sizeof *pager);
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

/*
returns a cursor pointing to a cell with matching key,
or if key wasn't found, the cell we could insert into.
*/
Cursor* table_find(Table* table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    Node* root_node = get_page(table->pager, root_page_num);
    if (root_node->common_header.type == NODE_INTERNAL) {
        return internal_node_find_leaf(table, root_page_num, key);
    } else {
        return leaf_node_find(table, root_page_num, key);
    }
}

Cursor* table_start(Table* table) {
    Cursor* cursor = table_find(table, 0);
    // NOTE: should probably place this code inside leaf_node_find
    LeafNode* first_leaf = (LeafNode*)get_page(table->pager, cursor->page_num);
    uint32_t num_cells = first_leaf->num_cells;
    cursor->end_of_table = (num_cells == 0);
    return cursor;
}

Cursor* table_end(Table* table) {
    // remember that `table_end` takes us to one step *past* the end of the table (where it's safe to insert).
    // this means that, for example, if current page has a limit number of cells, we'll be at limit+1
    Cursor* cursor = malloc(sizeof *cursor);
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    LeafNode* last_leaf = (LeafNode*)get_page(table->pager, table->root_page_num);
    cursor->cell_num = last_leaf->num_cells;
    cursor->end_of_table = true;

    return cursor;
}

Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);

    Table* table = (Table*)malloc(sizeof *table);
    table->pager = pager;
    table->root_page_num = 0;

    if (pager->num_pages == 0) {
        // new file - initialize page 0 as leaf node
        LeafNode* root_node = (LeafNode*)get_page(table->pager, 0);
        initialize_leaf_node(root_node);
        root_node->is_root = true;
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
    free(pager);
    free(table);
}

// get pointer to current row, create new page if needed.
SerializedRow* cursor_value(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    LeafNode* page = (LeafNode*)get_page(cursor->table->pager, page_num);
    return &(page->cells[cursor->cell_num]);
}

/* 
navigates leaf nodes.
if given cursor is at the last cell, goes to the 1st cell of the next node.
otherwise, goes to the next cell of the same node.
*/
void cursor_advance(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    LeafNode* node = (LeafNode*)get_page(cursor->table->pager, page_num);
    cursor->cell_num += 1;
    if (cursor->cell_num >= node->num_cells) {
        uint32_t next_page = node->next_leaf;
        /* NOTE: 
        an uninitialized `next_page` being set to 0 is only safe IFF `root_page_num == 0` always.
        if so, it's safe because `cursor_advance` navigates leaf pages. if there is more than one
        page, `root_node` is not a leaf node, so we can use it as an initializer.
        */
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
    // RESEARCH: WHY DOES THIS WORK?? // printf("%*d %*s %*s\n", row->id,  row->username, 25, row->email, 255);
}

void print_constants(void) {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("INTERNAL_NODE_MAX_KEYS: %d\n", INTERNAL_NODE_MAX_KEYS);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

// use a pager and page_num instead of a cursor because it's recursive - we'd have to copy cursor for each level
void print_tree(Pager* pager, uint32_t page_num, uint32_t indent_level) {
    Node* _node = get_page(pager, page_num);

    // indent(indent_level);
    printf("page %d/%d; ", page_num, TABLE_MAX_PAGES);
    if (_node->common_header.is_root) printf("root; ");
    switch (_node->common_header.type) {
    case NODE_INTERNAL: {
        InternalNode* node = (InternalNode*)_node;
        printf("internal; %d/%d keys\n", node->num_keys, INTERNAL_NODE_MAX_KEYS);
        // N keys == N+1 children
        for (uint32_t i = 0; i < node->num_keys; i++) {
            indent(indent_level+1);
            printf("+ key %d; ", node->_cells[i].key);
            print_tree(pager, node->_cells[i].child, indent_level+1);
        }
        indent(indent_level+1);
        printf("+ ");
        print_tree(pager, node->last_child, indent_level+1);
        break;
    } case NODE_LEAF: {
        LeafNode* node = (LeafNode*)_node;
        printf("leaf; %d/%d keys\n", node->num_cells, LEAF_NODE_MAX_CELLS);
        for (uint32_t i = 0; i < node->num_cells; i++) {
            indent(indent_level+1);
            printf("- key %d\n", node->cells[i].key);
        }
        break;
    }}
}

// void viz_tree(Pager* pager, uint32_t page_num, uint32_t indent_level) {
    
// }

void print_prompt(void) { printf("db > ");}

InputBuffer* new_input_buffer(void){
    InputBuffer* input_buffer = malloc(sizeof *input_buffer);
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
    input_buffer->input_length = bytes_read-1;
    // we can concat string by setting a new null termnator
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
#pragma GCC diagnostic pop    // NOTE: selecting all rows, for now
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

    LeafNode* node = (LeafNode*)get_page(table->pager, cursor->page_num);

    if (cursor->cell_num < node->num_cells) { // inserting between
        uint32_t key_at_index = node->cells[cursor->cell_num].key;
        if (key_at_index == key_to_insert) return EXECUTE_DUPLICATE_KEY;
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table){
    switch (statement->type){
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT:
            return execute_select(statement, table);
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

