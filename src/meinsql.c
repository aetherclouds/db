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

#include "btree.h"

#define log_success(format, ...) (use_color ? printf("\x1b[32m" format "\x1b[39m\n" __VA_OPT__(,) __VA_ARGS__) : printf(format"\n" __VA_OPT__(,) __VA_ARGS__))
#define log_error(format, ...) (use_color ? printf("\x1b[31m" format "\x1b[39m\n" __VA_OPT__(,) __VA_ARGS__) : printf(format"\n" __VA_OPT__(,)  __VA_ARGS__))

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
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    uint32_t num_rows;
    Pager* pager;
} Table;

typedef struct {
    Table* table;
    uint32_t row_num;
    bool end_of_table; // row_num is not an existing
} Cursor;

const uint32_t TABzLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;


bool use_color = true;


Pager* pager_open(const char* filename) {
    int fd = open(filename,
            O_RDWR | O_CREAT,
            S_IWUSR | S_IRUSR
    );
    if (fd == -1) {
        log_error("file could not be opened: %s", filename);
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    for (uint32_t i = 0; i++; i < TABLE_MAX_PAGES) {
        pager->pages[i] = NULL; // initially, no pages are loaded
    }
    return pager;
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        // program may follow this branch if flushing a page that hasn't been loaded to cache yet, but is stored no disk
        // (this happens because we measure how many pages to flush based on filesize, not )
        log_error("tried to flush null page: %d", page_num);
        // exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        log_error("error seeking; %d", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    if (bytes_written == -1) {
        log_error("failed writing to file: %d", errno);
        exit(EXIT_FAILURE);
    }
}

Cursor* table_start(Table* table) {
    Cursor* cursor = malloc(sizeof(Table));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = (table->num_rows == 0);

    return cursor;
}

Cursor* table_end(Table* table) {
    Cursor* cursor = malloc(sizeof(Table));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;

    return cursor;
}
Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;
    return table;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        pager->pages[i] = NULL;
    }
}

void db_close(Table* table) {
    Pager* pager = table->pager;
    uint32_t num_full_pages = pager->file_length / PAGE_SIZE;
    // first, we flush full pages, then a partial page.
    for (uint32_t i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == NULL) continue;

        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] == NULL;
    }
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        uint32_t page_idx = num_full_pages;
        pager_flush(pager, page_idx, num_additional_rows * ROW_SIZE);
        free(pager->pages[page_idx]);
        pager->pages[page_idx] == NULL;
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        log_error("failed to close db file");
        exit(EXIT_FAILURE);
    }
    // I don't get why we free all pages *again* on this part of the tutorial, so I'll just ignore it
    // ...
    free(pager);
    free(table);
}

void print_row(Row* row){
    // printf("%*d %*s %*s\n", 2, row->id, 32, row->username, 25, row->email);
    printf("%d %s %s\n",row->id, row->username, row->email);
    // TODO: WHY DOES THIS WORK?? // printf("%*d %*s %*s\n", row->id,  row->username, 25, row->email, 255);
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
        log_error("%s", "error reading input");
        exit(EXIT_FAILURE);
    }

    // remove newline
    // we can concat string by setting a new null termnator
    input_buffer->input_length = bytes_read-1;
    input_buffer->buffer[bytes_read-1] = '\0';
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    // if (strcmp(input_buffer->buffer, ".exit") == 0) {
    if (strncmp(input_buffer->buffer, ".exit", 5) == 0) {
        log_success("%s", "exiting");
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
        return META_COMMAND_SUCCESS;
    }

    return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    // int n_args_assigned = sscanf(input_buffer->buffer, "insert %d %33s %256s", &(statement->row_to_insert.id), &(statement->row_to_insert.username), &(statement->row_to_insert.email));
        statement->type = STATEMENT_INSERT;

        char* keyword = strtok(input_buffer->buffer, " ");
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

void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        log_error("tried to fetch a page number larger than max. allowed: %d > %d", page_num, TABLE_MAX_PAGES);
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
        // does this page already exist? (reminder that pages are 0 indexed so we check for equality as well)
        if (page_num <= num_pages) {
            uint32_t offset = page_num * ROWS_PER_PAGE * ROW_SIZE;
            lseek(pager->file_descriptor, offset, SEEK_SET);
            // we don't have to check if it crosses file size - implementation should set off-bounds bytes to 0
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("error reading file: %d", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

void* cursor_value(Cursor* cursor){
    // Get pointer to row_num, create new page if needed.
    uint32_t page_num = cursor->row_num / ROWS_PER_PAGE;
    // don't type yet because this page may not exist
    void* page = get_page(cursor->table->pager, page_num);
    uint32_t row_offset = cursor->row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;


    return page + byte_offset;
}

void* cursor_advance(Cursor* cursor){
    cursor->row_num += 1;
    if (cursor->row_num >= cursor->table->num_rows) cursor->end_of_table = true;
}

ExecuteResult execute_select(Statement* statement, Table* table){
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
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    Cursor* cursor = table_end(table);

    serialize_row(row_to_insert, cursor_value(cursor));
    table->num_rows += 1;

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
    };
    // gcc complains about this because https://stackoverflow.com/a/33607345
}


int main(int argc, char* argv[]) {
    if (argc < 2) {
        log_error("must provide a database filename");
        exit(EXIT_FAILURE);
    }
    struct option options[] = {
        {"no-color", no_argument, &use_color, false},
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
                    log_error("unrecognized meta-command: %s", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                log_error("unrecognized command: %s", input_buffer->buffer);
                continue;
            case PREPARE_SYNTAX_ERROR:
                log_error("incorrect syntax for valid command: %s", input_buffer->buffer);
                continue;
            case PREPARE_STRING_TOO_LONG:
                log_error("string too long for command: %s", input_buffer->buffer);
                continue;
            default:
                log_error("undocumented prepare error");
                continue;
        }

        switch (execute_statement(&statement, table)){
            case EXECUTE_SUCCESS:
                log_success("executed");
                break;
            case EXECUTE_FAILURE:
                log_error("failed to execute statement");
                continue;
            case EXECUTE_TABLE_FULL:
                log_error("table is full");
                continue;
        }
    }

    return 1;
}

