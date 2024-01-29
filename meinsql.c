#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>


#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100
// get sizeof on compile time for uninitialized structures
#define sizeof_ct(Struct, Attribute) sizeof(((Struct*)0)->Attribute)


typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
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
    // "_t" ensures cross-platform compatibility
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

typedef struct {
    uint32_t num_rows;
    void* pages[TABLE_MAX_PAGES];
} Table;


const uint32_t ID_SIZE = sizeof_ct(Row, id);
const uint32_t USERNAME_SIZE = sizeof_ct(Row, username);
const uint32_t EMAIL_SIZE = sizeof_ct(Row, email);
const uint32_t ROW_SIZE = sizeof(Row);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
// const uint32_t PAGE_SIZE = sysconf(_SC_PAGESIZE);
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;


void serialize_row(Row* source, void* destination) {
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
        printf("error reading input\n");
        exit(EXIT_FAILURE);
    }

    // remove newline
    // we can concat string by setting a new null termnator
    input_buffer->input_length = bytes_read-1;
    input_buffer->buffer[bytes_read-1] = '\0';
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        printf("\x1b[32m""exiting\n""\x1b[39m");
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
        return META_COMMAND_SUCCESS;
    }

    return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        // scanf reads from stdin, sscanf reads from given input
        int n_args_assigned = sscanf(input_buffer->buffer, "insert %d %32s %255s", &(statement->row_to_insert.id), &(statement->row_to_insert.username), &(statement->row_to_insert.email));
        if (n_args_assigned < 3) {
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if (strncmp(input_buffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void print_row(Row* row){
    printf("%*d %*s %*s\n", row->id, 2, row->username, 32, row->email, 25);
    // TODO: WHY DOES THIS WORK?? // printf("%*d %*s %*s\n", row->id,  row->username, 25, row->email, 255);
}

void* row_slot(Table* table, uint32_t row_num){
    // Get pointer to row_num, create new page if needed.
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    // don't type yet because this page may not exist
    void* page = table->pages[page_num];
    if (page == NULL){
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;

    return page + byte_offset;
}

ExecuteResult execute_select(Statement* statement, Table* table){
    // NOTE: selecting all rows, for now
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &(row));
        print_row(&row);
    }

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert(Statement* statement, Table* table){
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);

    serialize_row(row_to_insert, row_slot(table, table->num_rows));
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

Table* new_table() {
    Table* table = (Table*)malloc(sizeof(Table));
    table->num_rows = 0;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        table->pages[i] = NULL;
    }
    return table;
}

void free_table(Table* table) {

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        free(table->pages[i]);
    }
    free(table);
}

static inline void print_prompt(){ printf("db > ");}


int main(int argc, char* argv[]) {
    InputBuffer* input_buffer = new_input_buffer();
    Table* table = new_table();
    while (true) {
        print_prompt();
        read_input(input_buffer); // read into the buffer

        // we'll handle meta-commands (.) separately, so `continue` after processing
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("\x1b[31m""unrecognized meta-command: %s\n""\x1b[39m", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("\x1b[31m""unrecognized command: %s\n""\x1b[39m", input_buffer->buffer);
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf("\x1b[31m""incorrect syntax for valid command: %s\n""\x1b[39m", input_buffer->buffer);
                continue;
            default:
                printf("\x1b[31m""unknown error\n""\x1b[39m");
                continue;
        }

        switch (execute_statement(&statement, table)){
            case EXECUTE_SUCCESS:
                printf("\x1b[32m""executed\n""\x1b[39m");
                break;
            case EXECUTE_FAILURE:
                printf("\x1b[31m""failed to execute statement\n""\x1b[39m");
                continue;
            case EXECUTE_TABLE_FULL:
                printf("\x1b[31m""table is full\n""\x1b[39m");
                continue;
        }
    }
    
    return 1;
}