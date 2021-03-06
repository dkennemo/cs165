/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

// DK: NOTE that this is running on the SERVER. The client simply sends a message to the server
// and the server deconstructs it and runs it or decides the command doesn't conform somehow and kicks it
// out.

#define _BSD_SOURCE
#define FALSE 0
#define TRUE 1

#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
// #include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"
#include <errno.h>


extern int errno;

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

message_status parse_create_idx(char* create_arguments, Db* db_head) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* db_and_table_name = next_token(create_arguments_index, &status);
    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }
    // Get the column name free of quotation marks
    col_name = trim_quotes(col_name);
    printf("create column full name = %s\n", col_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(db_and_table_name) - 1;
    if (db_and_table_name[last_char] != ')') {
       return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    db_and_table_name[last_char] = '\0';
    char* table_name = malloc(100);
    strcpy(table_name, divide_period(db_and_table_name));
    printf("table name = %s\n", table_name);

    char* db_name = malloc(100);
    strcpy(db_name, db_and_table_name);
    printf("database name = %s\n", db_name);

    Db* db_search = malloc(sizeof(Db));
    db_search = lookup_db(db_name, db_head);
    printf("dbsearch name = %s and db_name = %s\n", db_search->name, db_name);
    if (strcmp(db_search->name,db_name) != 0) {
        printf("query unsupported. Bad db name\n");
        fflush(stdout);
        return QUERY_UNSUPPORTED;
        }
    // find the table in the db
    Table *table_search = malloc(sizeof(Table));
    table_search = lookup_table(table_name, db_head);
    printf("table search name = %s in db_name = %s\n", table_name, db_head->name);
    if (strcmp(table_search->name,table_name) != 0) {
        printf("query unsupported. Bad table name\n");
        fflush(stdout);
        return QUERY_UNSUPPORTED;
        }
    else
    {
        printf("calling routine to create columns ...\n");
        Status* ret_status = malloc(sizeof(Status));
        *ret_status = create_column(table_search, col_name, ret_status);
        free(ret_status);
    }
}


message_status parse_create_col(char* create_arguments, Db* db_head) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* db_and_table_name = next_token(create_arguments_index, &status);
    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }
    // Get the column name free of quotation marks
    col_name = trim_quotes(col_name);
    printf("create column full name = %s\n", col_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(db_and_table_name) - 1;
    if (db_and_table_name[last_char] != ')') {
       return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    db_and_table_name[last_char] = '\0';
    char* table_name = malloc(100);
    strcpy(table_name, divide_period(db_and_table_name));
    printf("table name = %s\n", table_name);

    char* db_name = malloc(100);
    strcpy(db_name, db_and_table_name);
    printf("database name = %s\n", db_name);

    //strcpy(table_name, db_and_table_name[last_char+1]);
    // look for period and create a separate db and table name
    //
    // PICK UP HERE
    //
    
    //char* table_name = divide_period(db_and_table_name);

    Db* db_search = malloc(sizeof(Db));
    db_search = lookup_db(db_name, db_head);
    printf("dbsearch name = %s and db_name = %s\n", db_search->name, db_name);
    if (strcmp(db_search->name,db_name) != 0) {
        printf("query unsupported. Bad db name\n");
        fflush(stdout);
        return QUERY_UNSUPPORTED;
        }
    // find the table in the db
    Table *table_search = malloc(sizeof(Table));
    table_search = lookup_table(table_name, db_head);
    printf("table search name = %s in db_name = %s\n", table_name, db_head->name);
    if (strcmp(table_search->name,table_name) != 0) {
        printf("query unsupported. Bad table name\n");
        fflush(stdout);
        return QUERY_UNSUPPORTED;
        }
    else
    {
        printf("calling routine to create columns ...\n");
        Status* ret_status = malloc(sizeof(Status));
        *ret_status = create_column(table_search, col_name, ret_status);
        free(ret_status);
    }
}
    // turn the string column count into an integer, and check that the input is valid.
/*    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Table* create_status = malloc(sizeof(Table));
    create_status = lookup_table(table_name, db_search);
    if (create_status == NULL) {
        printf("Table doesn't exist, so creating it now\n");
        Status* result = malloc(sizeof(Status));
        create_table(db_search, table_name, col_cnt, result);
        free(result);
        printf("created table - yay!\n");
        return OK_DONE;
    }
    return OK_DONE;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/


// THIS WAS THE ORIGINAL PARSE_SELECT FXN - USE THIS ONE OF THE OTHER DOESN'T WORK!
/*
message_status parse_select(char* create_arguments, Db* db_head, Var* var_pool) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    db_name = trim_quotes(db_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    printf("Table name to create is %s\n", table_name);
    // check that the database argument is the current active database
    // DK: Change this somehow to accomodate MORE THAN ONE database. Idea:
    // create a linked list of databases ending in NULL - traverse the list to see
    // if the database specified is in the list. If so, good. Otherwise, error.
    Db* db_search = malloc(sizeof(Db));
    db_search = lookup_db(db_name, db_head);
    printf("dbsearch name = %s and db_name = %s\n", db_search->name, db_name);
    if (strcmp(db_search->name,db_name) != 0) {
        printf("query unsupported. Bad db name\n");
        fflush(stdout);
        return QUERY_UNSUPPORTED;
        }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Table* create_status = malloc(sizeof(Table));
    create_status = lookup_table(table_name, db_search);
    if (create_status == NULL) {
        printf("Table doesn't exist, so creating it now\n");
        Status* result = malloc(sizeof(Status));
        create_table(db_search, table_name, column_cnt, result);
        free(result);
        printf("created table - yay!\n");
        return OK_DONE;
    }
    return OK_DONE;
}
*/

message_status parse_create_tbl(char* create_arguments, Db* db_head) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    db_name = trim_quotes(db_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    printf("Table name to create is %s\n", table_name);
    // check that the database argument is the current active database
    // DK: Change this somehow to accomodate MORE THAN ONE database. Idea:
    // create a linked list of databases ending in NULL - traverse the list to see
    // if the database specified is in the list. If so, good. Otherwise, error.
    Db* db_search = malloc(sizeof(Db));
    db_search = lookup_db(db_name, db_head);
    printf("dbsearch name = %s and db_name = %s\n", db_search->name, db_name);
    if (!db_search || !db_search->name) {
        printf("Db %s does not exist. Creating it.\n", db_name);
        Var* var_pool = malloc(sizeof(Var));
        db_search = create_db(db_name, db_head, var_pool);
        }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Table* create_status = malloc(sizeof(Table));
    printf("going into lookup_table with table name %s and db_search %s\n", table_name, db_search);
    bool found = malloc(sizeof(bool)); 
    create_status = lookup_table(table_name, db_search);

    if (create_status == NULL) {
        printf("Table doesn't exist, so creating it now\n");
        Status* result = malloc(sizeof(Status));
        create_table(db_search, table_name, column_cnt, result);
        free(result);
        printf("created table - yay!\n");
        return OK_DONE;
    }
    return OK_DONE;
}


/*
void shutdown(Db* db_head) {
    if (db_head == NULL) {
        printf("Db list is empty - nothing to save to disk\n");
    }
    else {
        Db* current_db = db_head;
        do
        {
            printf("Database %s:\n", current_db->name);
            // Open a new .dsl file called current_db->name && ".dsl", for writing
F            if (current_db->tables == NULL) printf("  Tables list is empty.\n");
            else {
                Table* current_table = current_db->tables;
                Column** = 
                do
                {
                    printf("  Table %s:\n", current_table->name);
                    if (current_table->columns == NULL) printf("    Column list is empty.\n");
                    else {
                        Column* current_col = current_table->columns;
                        int col_number = 1;
                        do
                        {
                            fprintf(fp, "%s.%s.%s", current_db->name, current_table->name, current_table->columns);
                            current_col = current_col->next_col; 
                            if (current_col != NULL)
                                fprintf(",");
                            else
                                fprintf("\n");
                        }
                        while (current_col != NULL);
                    }
                }




                            printf("    Column %s (%i):\n", current_col->name, col_number++);
                            if (current_col->data == NULL) printf("      No data in this column.\n");
                            else {
                                printf("    ");
                                int_list* current_data = current_col->data;
                                do
                                {
                                    printf(" %i ", current_data->item);
                                    current_data = current_data->next;
                                }
                                while (current_data != NULL);
                                printf("\n");
                            }
                    }
                current_table = current_table->next_tbl;
                }
                while (current_table != NULL);
            }
            current_db = current_db->next_db;
        }
        while (current_db != NULL);
    }
}
*/

void print_db(Db* db_head) {
    if (db_head == NULL) 
        printf("Db list is empty.\n");
    else {
        Db* current_db = db_head;
        do
        {
            printf("Database %s:\n", current_db->name);
            if (current_db->tables == NULL) printf("  Tables list is empty.\n");
            else {
                Table* current_table = current_db->tables;
                do
                {
                    printf("  Table %s:\n", current_table->name);
                    if (current_table->columns == NULL) printf("    Column list is empty.\n");
                    else {
                        Column* current_col = current_table->columns;
                        int col_number = 1;
                        do
                        {
                            printf("    Column %s (%i) addr: %p:\n", current_col->name, col_number++, (void*)current_col);

                            if (current_col->data == NULL) printf("      No data in this column.\n");
                            else {
                                printf("    ");
                                int_list* current_data = current_col->data;
                                do
                                {
                                    printf(" %i ", current_data->item);
                                    current_data = current_data->next;
                                }
                                while (current_data != NULL);
                                printf("\n");
                            }
                        current_col = current_col->next_col; 
                        }
                        while (current_col != NULL);
                    }
                current_table = current_table->next_tbl;
                }
                while (current_table != NULL);
            }
            current_db = current_db->next_db;
        }
        while (current_db != NULL);
    }
}

message_status parse_lookup_col(char* lookup_arguments, Db* curr_db) {
    char *token;
    //message_status mes_status;
    token = strsep(&lookup_arguments, ",");
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        char* col_name = token;
        col_name = trim_quotes(col_name);
        int last_char = strlen(col_name) - 1;
        if (last_char < 0 || col_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
    col_name[last_char] = '\0';
    printf("Col to lookup is %s\n", col_name);
    Table* table = curr_db->tables;
    lookup_column(col_name, table);
    // Note: this will only handle the case of ONE table. If you have two tables, it won't traverse; revise.
    return OK_DONE;
    }
}

message_status parse_lookup_tbl(char* lookup_arguments, Db* curr_db) {
    char *token;
    //message_status mes_status;
    token = strsep(&lookup_arguments, ",");
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        char* tbl_name = token;
        tbl_name = trim_quotes(tbl_name);
        int last_char = strlen(tbl_name) - 1;
        if (last_char < 0 || tbl_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        tbl_name[last_char] = '\0';
        printf("Tbl name to lookup is %s\n", tbl_name);
        lookup_table(tbl_name, curr_db);
        return OK_DONE;
        }
    }

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

message_status parse_lookup_db(char* lookup_arguments, Db* db_head) {
    char *token;
    // message_status mes_status;
    token = strsep(&lookup_arguments, ",");
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        char* db_name = token;
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        db_name[last_char] = '\0';
        printf("Db name to lookup is %s\n", db_name);

        lookup_db(db_name, db_head);
        return OK_DONE;
        }
    }


message_status parse_lookup(char* create_arguments, Db* db_head) {
    message_status mes_status;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return mes_status;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                printf("Been asked to lookup a db\n");
                mes_status = parse_lookup_db(tokenizer_copy, db_head);
            } else if (strcmp(token, "tbl") == 0) {
                printf("Been asked to lookup a tbl\n");
                mes_status = parse_lookup_tbl(tokenizer_copy, db_head);
            } else if (strcmp(token, "col") == 0) {
                printf("Been asked to lookup a col\n");
                mes_status = parse_lookup_col(tokenizer_copy, db_head);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return mes_status;
}

message_status parse_create_db(char* create_arguments, Db* db_head, Var* var_pool) {
    char *token;
    // Status mes_status;
    // message_status mes_status;
    // struct Db* db;
    // Status stat;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';
        printf("Db name to create is %s\n", db_name);

        //token = strsep(&create_arguments, ",");
        //if (token != NULL) {
        //    return INCORRECT_FORMAT;
        create_db(db_name, db_head, var_pool);
        return OK_DONE;
    }
}


/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
message_status parse_create(char* create_arguments, Db* db_head, Var* var_pool) {
    message_status mes_status;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return mes_status;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                printf("Been asked to create a db\n");
                mes_status = parse_create_db(tokenizer_copy, db_head, var_pool);
            } else if (strcmp(token, "tbl") == 0) {
                printf("Been asked to create a tbl\n");
                mes_status = parse_create_tbl(tokenizer_copy, db_head);
            } else if (strcmp(token, "idx") == 0) {
                printf("Been asked to create an index\n");
                mes_status = parse_create_idx(tokenizer_copy, db_head);
            } else if (strcmp(token, "col") == 0) {
                printf("Been asked to create a col\n");
                mes_status = parse_create_col(tokenizer_copy, db_head);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return mes_status;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* load_db(char* dbFile, Db* db_head, Var* var_pool, int client_socket, struct ClientContext* context, int* batch_mode, Batch_list* batch) {
    char* query_command = malloc(100);
    char* query_command_backup = query_command;
    char* start_counter;
    message *send_message;

    // PARSING SECTION

    // skip prens
    if (dbFile[0] == '(')
        dbFile++;
    int last_char = strlen(dbFile);
        dbFile[last_char - 1] = '\0';
    dbFile = trim_quotes(dbFile);
    printf("opening %s\n", dbFile);


    // open the file for reading and writing
    FILE* database = fopen(dbFile, "r");
    if (database == NULL) {
        //int errnum = errno;
        //printf(stderr, "Error opening file: %s\n", strerror(errnum));
        return NULL;
    };

    // There are two styles of files that can be loaded, .csv and .dsl. They are treated differently.
    // Logic: Is it a csv file or a dsl file? If not, return an error.
    char filetype[4], db_name[100], tbl_name[100], col_name[100];

    // Find suffix
    strcpy(filetype, dbFile + strlen(dbFile) - 3);
    char* linecounter;
    int end_of_line = 0;

    // Is it csv?
    if (strncmp(filetype, "csv", 3) == 0)
    {
        
        DbTblCol* roster = malloc(sizeof(DbTblCol));
        roster->items = malloc(sizeof(int_list*));
        roster->items = NULL;
        DbTblCol* roster_start = roster;
        // read a line of data, store in query_command
        fgets(query_command, 100, database);
        printf("read first line: %s\n", query_command);
        while (!end_of_line) {
            linecounter = strchr(query_command, '.');
            *linecounter = '\0';
            strcpy(roster->db_name, query_command);
            printf("db name is %s  ", roster->db_name);
            query_command = linecounter + 1;
            linecounter = strchr(query_command, '.');
            start_counter = query_command;
            *linecounter = '\0';
            query_command = (linecounter + 1);
            strcpy(roster->tbl_name, start_counter);
            printf("table name is %s  ", roster->tbl_name);
            linecounter = strchr(query_command, ',');
            col_name[strlen(col_name)] = '\0';
            if (linecounter == NULL)
            {
                strcpy(roster->col_name, query_command);
                printf("col name is %s\n", roster->col_name);
                end_of_line = 1;                    // signals this is the last item in the line to process.
                roster->next = NULL;
            }
            else {
                *linecounter = '\0';
                query_command = trim_whitespace(query_command);
                strcpy(roster->col_name, query_command);
                printf("col name is %s\n", roster->col_name);
                query_command = linecounter + 1;
                DbTblCol* new_roster = malloc(sizeof(DbTblCol));
                roster->next = new_roster;
                roster = new_roster;
            };
        };
        roster = roster_start;                                      // reset roster back to beginning of roster list

        Table *insert_table = malloc(sizeof(Table*));                // create an insert table pointer object
        insert_table = lookup_table(roster->tbl_name, db_head);   // look up the table name in the roster
        query_command = query_command_backup;                        // point the query command pointer to the original (since it's moved during the line scan process)
        int insert_val;                                             // this is the integer representation of the numbers read off the csv file

        if (!feof(database)) do                                      // while eof hasn't been reached...
        {
            *query_command = '\0';
            fgets(query_command, 100, database);                    // read a new line from the file, place in query_command after some pre-processing
            query_command = trim_whitespace(query_command);         // trim any excess whitespace            
            if (*query_command == '\0') break;
            printf("new line: %s\n", query_command);
            roster = roster_start;                                  // go to the beginning of the roster of db/table/column inserts
            Column* insert_column = insert_table->columns;          // Now create an insert column pointer and point it to insret_table's first column
            do {                                                     // begin loop
                int_list* start = malloc(sizeof(int_list*));          // Now create an int_list node pointer called start
                query_command[strcspn(query_command, ",")] = '\0';      // change the comma to a null terminator
                //char* next_char = strsep(query_command, '\0') + 1;
                insert_val = atoi(query_command);                       // collect the val preceding the null terminator and change to an int, store in insert_val
                printf("v: %i\n", insert_val);                          // display the value just read
                query_command += strcspn(query_command, ",") + 1;       // move the query command pointer to just after the null terminator
                int_list* new_int = malloc(sizeof(int_list));           // now create a new int_list object to store it in
                new_int->item = insert_val;                             // store the value
                printf("going to lookup column fxn with %s and %p parms\n", roster->col_name, insert_table);
                fflush(stdout);
                insert_column = lookup_column(roster->col_name, insert_table);
                if (insert_column->data == NULL)                        // is column data empty? If so, point it to the int_list just created
                    insert_column->data = new_int;
                else 
                    {
                        int_list* search_list = insert_column->data;
                        while (search_list->next != NULL) 
                            search_list = search_list->next;
                        search_list->next = new_int;
                    }
                roster = roster->next;
                } while (strcspn(query_command, ",") != 0);
        query_command = query_command_backup;
        }
        while (!feof(database));
    }
    else if (strncmp(filetype, "dsl", 3) == 0)
    {
        //DbOperator* x;
        if (!feof(database)) do
        {
            fgets(query_command, 100, database);
            parse_command(query_command, send_message, client_socket, context, db_head, var_pool, batch_mode, batch);
        }
        while (!feof(database));
    };
    // If csv, then:
    //      Read first line
    //      The first line is a series of one or more of: <database>.<table>.<column>
    //      For first combo, Create database, then create a table in the database, then create column.
    //      Create a linked list with a series of column pointers. Add pointer for this first column to the list.
    //      Until the end of line,
    //          Read the next set of <d>.<t>.<c>
    //          Create column within <d> and <t>
    //          Add column pointer to the column pointer list
    //      Until the end of file
    //          Read next line
    //          Add each value in the line to the nth column. Set a data item pointer to track where in the list you are.
    //      At the end of the file, end each data list with NULL.
    //  Else if dsl then:
    //      Until end of file
    //          Read line
    //          parse and execute using parse command.          

    printf("Reading db completed.\n");
    fclose(database);
    return NULL;
}

DbOperator* parse_insert(char* query_command, message* send_message, Db* db_head) {
    //unsigned int columns_inserted = 0;
    char* token = malloc(100);
    token[0] = NULL;
    char* db_name = malloc(100);
    db_name[0] = '\0';
    char* table_name = malloc(100);
    char** command_index;
    int last_char;
    char* db_insert = malloc(100);
    db_insert[0] = '\0';
    int insert_val = malloc(sizeof(int));

    // pre-process query command to exclude prens
    if (query_command[0] == '(') {
        query_command++;
        command_index = &query_command;
        // parse table input
        db_name = next_token(command_index, &send_message->status);
        last_char = strlen(db_name);
        db_name[last_char] = '\0';

        // get db name and table name
        strcpy(table_name, divide_period(db_name));
        printf("divided db and table = %s and %s\n", db_name, table_name);

        //
        //query_command += last_char + 2;
        printf("start of variable list = %s", query_command);
        //

        // establish table pointer and lookup table listed
        Table* insert_table = malloc(sizeof(Table*));               // declare a table pointer called insert_table 
        bool found = malloc(sizeof(bool));
        insert_table = lookup_table(table_name, db_head);    // lookup the table by name and place the pointer in insert_table    

        // check to make sure table actually exists, exit if no.
        if (insert_table == NULL) {
            printf("couldn't find table so exiting out\n");
            return NULL;
        }

        Column* insert_column = insert_table->columns;              // Now create an insert column pointer and point it to insret_table's first column 

        int_list* start = malloc(sizeof(int_list*));                // Now create an int_list node pointer called start
        while (strcspn(query_command, ",") != 0)                    // as long as there is a comma to be found in query command...
        {

            query_command[strcspn(query_command, ",")] = '\0';      // change the comma to a null terminator
            //char* next_char = strsep(query_command, '\0') + 1;
            insert_val = atoi(query_command);                       // collect the val preceding the null terminator and change to an int, store in insert_val
            query_command += strcspn(query_command, ",") + 1;       // move the query command pointer to just after the null terminator
            printf("interpreted atoi result as %i\n", insert_val);  // display the value just read
            int_list* new_int = malloc(sizeof(int_list));           // now create a new int_list object to store it in
            new_int->item = insert_val;                             // store the value
            if (insert_column->data == NULL)                        // is column data empty? If so, point it to the int_list just created
                insert_column->data = new_int;
            else {
                int_list* search_list = insert_column->data;
                while (search_list->next != NULL) {
                    search_list = search_list->next;
                }
                search_list->next = new_int;
            };
        insert_column = insert_column->next_col;
        };
    }
return NULL;
}


/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 * 
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse? 
 *      What if such command requires multiple arguments?
 **/

int_list* parse_fetch(char* query_command, char* handle, Db* db_head, Var* var_pool) {
    char* period_pointer = strchr(query_command, '.');
    char* comma_pointer;
    char* pren_pointer;
    char* db_name = query_command;
    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };
    printf("tracking down db: %s\n", db_name);
    period_pointer = strchr(query_command, '.');
    char* table_name = query_command;
    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };
    printf("tracking down table: %s\n", table_name);
    comma_pointer = strchr(query_command, ',');
    char* column_name = query_command;
    if (comma_pointer != NULL) 
    {
        *comma_pointer = '\0';
        query_command = ++comma_pointer;
    };
    comma_pointer = strchr(query_command, ')');
    char* var_name = query_command;
    if (comma_pointer != NULL) 
    {
        *comma_pointer = '\0';
        query_command = ++comma_pointer;
    };
    printf("variable name for fetch indexes = %s\n", var_name);
    pren_pointer = strchr(query_command, ')');
    printf("Assigning to handle: %s\n", handle);
    int_list* fetch_list = fetch_row(db_name, table_name, column_name, var_name, db_head, handle, var_pool);
    return fetch_list;
}


int_list* parse_select(char* query_command, char* handle, Db* db_head, Var* var_pool) {
    char* period_pointer = strchr(query_command, '.');
    char* comma_pointer;
    char* pren_pointer;
    char* db_name = query_command;
    int low_value, high_value;
    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };
    printf("tracking down db: %s\n", db_name);
    period_pointer = strchr(query_command, '.');
    char* table_name = query_command;
    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };
    printf("tracking down table: %s\n", table_name);
    comma_pointer = strchr(query_command, ',');
    char* column_name = query_command;
    if (comma_pointer != NULL) 
    {
        *comma_pointer = '\0';
        query_command = ++comma_pointer;
    };
    comma_pointer = strchr(query_command, ',');
    char* low_string = query_command;
    if (comma_pointer != NULL) 
    {
        *comma_pointer = '\0';
        if (strcmp(low_string, "null") == 0)
            low_value = -2147483647;
        else
            low_value = atoi(low_string);
        query_command = ++comma_pointer;
    };
    printf("low value = %s\n", low_string);
    pren_pointer = strchr(query_command, ')');
    char* high_string = query_command;
    if (pren_pointer != NULL) 
    {
        *pren_pointer = '\0';
        if (strcmp(high_string, "null") == 0)
            high_value = 2147483647;
        else
            high_value = atoi(high_string);
    };
    printf("high_value = %s\n", high_string);
    printf("Assigning to handle: %s\n", handle);
    int_list* selection = select_row(db_name, table_name, column_name, low_value, high_value, db_head, handle, var_pool);
    return selection;
}

DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context, Db* db_head, Var* var_pool, int* batch_mode, Batch_list* batch) 
{
    int_list* result = malloc(sizeof(int_list));
    DbOperator *dbo = malloc(sizeof(DbOperator)); 
    //printf("query is: %s\n", query_command);

    // if leads with -- is a comment and should be ignored
    if (strncmp(query_command, "--", 2) == 0) 
        return NULL;

    if (*batch_mode == 1)
    {
        Batch_list* new_batch;
        //Batch_list* batch;
        if (batch == NULL) {
            Batch_list* new_batch = malloc(sizeof(Batch_list));
            strcpy(new_batch->query, query_command);
            new_batch->next = NULL;
            batch = new_batch;
            printf("new batch list - single item: %s", batch->query);
        } else {
            Batch_list* batch_working_copy = batch;
            printf("traversing batch list: \n");
            while (batch_working_copy != NULL) {
                printf("batch item: %s\n", batch_working_copy->query);
                batch_working_copy = batch_working_copy->next;
            };
            printf("newest item: %s\n", new_batch->query);
            batch_working_copy = new_batch;
        };
    } 
    else if (*batch_mode == 1 && batch != NULL && (strncmp(query_command, "batch_execute()", 15) == 0)) {
        *batch_mode = 0;
        while(batch != NULL) {
            parse_command(batch->query, send_message, client_socket, context, db_head, var_pool, batch_mode, batch);
            Batch_list* batch_old = batch;
            batch = batch->next;
            free(batch_old);
        };
    }
    else {

    // look for an equals sign, indicating a handle
    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) 
    {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;

    } 
    else 
        handle = NULL;

    // prep query command for passing to other routines for further parsing and execution.
    query_command = trim_whitespace(query_command);
    if (!handle) {
        if (strncmp(query_command, "create", 6) == 0) 
        {
            query_command += 6;
            printf("create command invoked\n");
            //send_message->status = parse_create(query_command, db_head, var_pool);
            parse_create(query_command, db_head, var_pool);
            dbo = malloc(sizeof(DbOperator));
            dbo->type = CREATE;
        }; 
        if (strncmp(query_command, "relational_insert", 17) == 0) 
        {
            query_command += 17;
            parse_insert(query_command, send_message, db_head);
        }; 
        if (strncmp(query_command, "load", 4) == 0) 
        {
            query_command += 4;
            dbo = load_db(query_command, db_head, var_pool, client_socket, context, batch_mode, batch);
        }; 
        if (strncmp(query_command, "lookup", 6) == 0) 
        {
            query_command += 6;
            message_status mes = parse_lookup(query_command, db_head);
            //dbo->type = LOOKUP;
        }; 
        if (strncmp(query_command, "print_db", 8) == 0) 
        {
            query_command += 8;
            print_db(db_head);
        }; 
        if (strncmp(query_command, "print", 5) == 0) 
        {
            query_command += 5;
            print_var(var_pool, query_command);
        }; 
        if (strncmp(query_command, "batch_queries()", 15) == 0)
            {
                printf("Requested to enter batch mode\n");
                if (*batch_mode == 1) {
                    printf("Already in batch mode - cannot re-enter until batch_execute has occurred.\n");
                }
                else {
                    printf("Entering batch mode. No further activity - except shutdown - will occur until batch_execute occurs.\n");
                    *batch_mode = 1;
                    Batch_list* batch = malloc(sizeof(Batch_list));
                    Batch_list* batch_start = batch;
                }

            }

    };

    if (handle) 
    {
        char *open_pren = strchr(query_command, '(');       // find open pren
        if (open_pren == NULL) 
        {
            dbo->type = NA;
            printf("No valid command entered.\n");
            return dbo;
        }
        else 
        {
            *open_pren = '\0';
            //printf("testing: is join in command %s...\n", query_command);
            if (strncmp(query_command, "join", 4) == 0) 
            {
                query_command += 5;         // skip over join leters + open pren
                printf("PLACEHOLDER: Insert JOIN logic here\n");
            };
            //printf("testing: is select in command %s...\n", query_command);
            if (strncmp(query_command, "select", 6) == 0) 
            {
                query_command += 7;         // skip over select letters + open pren
                result = parse_select(query_command, handle, db_head, var_pool);
                declare_handle(handle, result, var_pool);
            };
            if (strncmp(query_command, "fetch", 5) == 0) 
            {
                query_command += 6;         // skip over fetch letters + open pren
                result = parse_fetch(query_command, handle, db_head, var_pool);
                declare_handle(handle, result, var_pool);
            }; 
            if (strncmp(query_command, "min", 3) == 0) 
            {
                query_command += 4;
                printf("min query command is now: %s\n", query_command);
                char* comma_pointer = strchr(query_command, ",");
                if (comma_pointer == NULL) 
                {
                    printf("no comma detected\n");
                    char* close_pren = strchr(query_command, ')');
                    if (close_pren == NULL) 
                    {
                        printf("No close pren - Improper format for min command\n");
                        dbo->type = NA;
                        return dbo;
                    }
                    else 
                    {
                        printf("now analyzing min\n");
                        int_list* result = malloc(sizeof(int_list));
                        result->item = find_min(query_command, 0, db_head, var_pool);
                        result->next = NULL;
                        declare_handle(handle, result, var_pool);
                        printf("min is %i\n", result->item);
                    };
                }
                else 
                {
                    char* close_pren = strchr(query_command, ")");
                    if (close_pren == NULL) 
                    {
                        printf("No close pren - Improper format for min command\n");
                        dbo->type = NA;
                        return dbo;
                    };
                    printf("now analyzing min\n");
                    int_list* result = malloc(sizeof(int_list));
                    result->item = find_min(query_command, comma_pointer + 1, db_head, var_pool);
                    declare_handle(handle, result, var_pool);
                    printf("min is %i\n", result->item);
                };
            }; 
            if (strncmp(query_command, "max", 3) == 0) 
            {
                query_command += 4;
                char* comma_pointer = strchr(query_command, ',');
                if (comma_pointer == NULL) 
                {
                    char* close_pren = strchr(query_command, ')');
                    if (close_pren == NULL) 
                    {
                        printf("Improper format for max command\n");
                        dbo->type = NA;
                        return dbo;
                    }
                    else 
                    {
                        int_list* result = malloc(sizeof(int_list));
                        result->item = find_max(query_command, NULL, db_head, var_pool);
                        result->next = NULL;
                        declare_handle(handle, result, var_pool);
                    };
                }
                else 
                {
                    char* close_pren = strchr(query_command, ')');
                    if (close_pren == NULL) 
                    {
                        printf("Improper format for max command\n");
                        dbo->type = NA;
                        return dbo;
                    };
                    int_list* result = malloc(sizeof(int_list));
                    result->item = find_max(query_command, comma_pointer + 1, db_head, var_pool);
                    declare_handle(handle, result, var_pool);
                    printf("MAX is %i\n", result->item);
                };
            };
            if (strncmp(query_command, "sum", 3) == 0) 
            {
                query_command += 4;
                char* close_pren = strchr(query_command, ')');
                if (close_pren == NULL) 
                {
                    printf("Improper format for sum command\n");
                    dbo->type = NA;
                    return dbo;
                }
                else 
                {
                    int_list* result = malloc(sizeof(int_list));
                    result->next = NULL;
                    result->item = find_sum(query_command, db_head, var_pool);
                    declare_handle(handle, result, var_pool);
                    printf("SUM is %i\n", result->item);
                };
            };
            if (strncmp(query_command, "avg", 3) == 0) 
            {
                query_command += 4;
                char* close_pren = strchr(query_command, ')');
                if (close_pren == NULL) 
                {
                    printf("Improper format for avg command\n");
                    dbo->type = NA;
                    return dbo;
                }
                else 
                {
                    int_list* result = malloc(sizeof(int_list));
                    result->next = NULL;
                    result->item = find_avg(query_command, db_head, var_pool);
                    printf("AVG is %i\n", result->item);
                    declare_handle(handle, result, var_pool);
                };
            };
            if (strncmp(query_command, "add", 3) == 0) 
            {
                printf("Requested add");
                query_command += 4;
                char* comma_pointer = strchr(query_command, ',');
                if (comma_pointer == NULL) 
                {
                    printf("Improper format for vector add command\n");
                    dbo->type = NA;
                    return dbo;
                }
                else 
                {
                    char* col2 = malloc(100);
                    strcpy(col2, divide_comma(query_command));
                    int_list* result = malloc(sizeof(int_list));
                    result = find_add(query_command, col2, db_head, var_pool);
                    declare_handle(handle, result, var_pool);
                };
            };
            if (strncmp(query_command, "sub", 3) == 0) 
            {
                printf("Requested sub");
                query_command += 4;
                char* comma_pointer = strchr(query_command, ',');
                if (comma_pointer == NULL) 
                {
                    printf("Improper format for vector add command\n");
                    dbo->type = NA;
                    return dbo;
                }
                else 
                {
                    char* col2 = malloc(100);
                    strcpy(col2, divide_comma(query_command));
                    int_list* result = malloc(sizeof(int_list));
                    result = find_sub(query_command, col2, db_head, var_pool);
                    declare_handle(handle, result, var_pool);
                };
            };
        };
    };
};
}
