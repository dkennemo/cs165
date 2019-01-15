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



message_status parse_select(char* create_arguments, Db* db_head) {
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
    message_status mes_status;
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
    lookup_column(col_name, curr_db);
    return OK_DONE;
    }
}

message_status parse_lookup_tbl(char* lookup_arguments, Db* curr_db) {
    char *token;
    message_status mes_status;
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
    message_status mes_status;
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
    Status mes_status;
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
        mes_status = create_db(db_name, db_head, var_pool);
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

DbOperator* load_db(char* dbFile, Db* db_head, Var* var_pool, int client_socket, struct ClientContext* context) {
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
        int errnum = errno;
        printf(stderr, "Error opening file: %s\n", strerror(errnum));
        return(1);
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
            strcpy(db_name, query_command);
            strcpy(roster->db_name, db_name);
            printf("db name is %s\n", roster->db_name);
            query_command = linecounter + 1;
            linecounter = strchr(query_command, '.');
            start_counter = query_command;
            *linecounter = '\0';
            query_command = (linecounter + 1);
            strcpy(tbl_name, start_counter);
            strcpy(roster->tbl_name, tbl_name);
            printf("table name is %s\n", roster->tbl_name);
                        
            linecounter = strchr(query_command, ',');
            if (linecounter == NULL)
            {
                strcpy(col_name, query_command);
                strcpy(roster->col_name, col_name);
                printf("col name is %s\n", roster->col_name);
//              Status status = insert_row(roster->db_name, Db* db_head, roster->table_name, roster->col_number, struct int_list* list, 
//                  Status *ret_status)
                end_of_line = 1;                    // signals this is the last item in the line to process.
                roster->next = NULL;
            }
            else {
                *linecounter = '\0';
                strcpy(col_name, query_command);
                strcpy(roster->col_name, col_name);
                printf("col name is %s\n", roster->col_name);
                query_command = linecounter + 1;
                DbTblCol* new_roster = malloc(sizeof(DbTblCol));
                roster->next = new_roster;
                roster = new_roster;
            };
        };
        // create db, re-iterate through roster and create table and columns
        Status mes_status = create_db(roster->db_name, db_head, var_pool);
        int num_columns = 0;

        // Count columns by traversing roster nodes.
        roster = roster_start;
        while (roster) {
            num_columns++;
            roster = roster->next;
        };

        // Create table by traversing roster nodes.
        Status* ret_status = malloc(sizeof(Status));
        roster = roster_start;
        printf("roster->tbl_name to pass is %s\n", roster->tbl_name);
        mes_status = create_table(db_head, roster->tbl_name, num_columns, ret_status);
        free(ret_status);

        // For each column, find the right table and create a new column
        roster = roster_start;
        int_list* current = malloc(sizeof(int_list));
        Table *table_search = malloc(sizeof(Table));
        for (int y = 0; y < num_columns; y++) {
            table_search = lookup_table(roster->tbl_name, db_head);
            *ret_status = create_column(table_search, roster->col_name, ret_status);    
            roster = roster->next;
        }
        char data_item[10];
        int data_item_int;
        Table* table_search_backup = table_search;
        Column* col_current = malloc(sizeof(Column*));       
        col_current = table_search->columns;
        col_current->data = table_search->columns->data;
        Column* col_start = table_search->columns;
        col_start->data = table_search->columns->data;
        int_list* latest_item = malloc(sizeof(int_list));



        if (!feof(database)) do 
        {
            //read a new line from the file, place in query_command after some pre-processing
            int_list* latest_item = malloc(sizeof(int_list*));
            int_list* next = malloc(sizeof(int_list*));
            next = NULL;
            latest_item->next = next;
            latest_item = NULL;
            query_command = query_command_backup;
            *query_command = '\0';
            fgets(query_command, 100, database);
            col_current = col_start;
            query_command = trim_whitespace(query_command);
            printf("new line: %s\n", query_command);
            // reset roster pointer to front of list.
            roster = roster_start;
            // only process further if roster is non-empty AND length of query command > 0
            while (roster && strlen(query_command) > 0)
            {
                // get the first item in the line, convert to an int and place in data_item_int
                linecounter = strchr(query_command, ',');
                if (linecounter) *linecounter = '\0';
                start_counter = query_command;
                strcpy(data_item, start_counter);
                data_item_int = atoi(data_item);
                printf("data item = %i \n", data_item_int);
                // create a new int_list called current, point it to the list associated with each roster.
                int_list* current = roster->items;

                printf("roster = %i\n", roster);
                //printf("items = %i\n", current);
                // is the current pointer NULL (that is, is the list of items for that roster item empty?)
                if (!current) {
                    // if yes, create a new int_list called new_item
                    int_list* new_item = malloc(sizeof(int_list));
                    // set the COLUMN pointer to the new item.
                    printf("col_current->data = %i\n", col_current->data);
                    latest_item = col_current->data;
                    latest_item->next = col_current->data->next;
                    while (latest_item->next != NULL)
                        latest_item = latest_item->next;
                    latest_item = new_item;
                    // store the data_item_int in the new item (to which the column pointer points)
                    new_item->item = data_item_int;
                    // ... and set the next pointer of the int list to null.
                    new_item->next = NULL;
                    // go to the next roster
                    roster = roster->next;
                    col_current = col_current->next_col;
                }
                else
                while (current != NULL) {
                    // if no, then there's a list to traverse.
                    // is the indicated next int list is NULL (that is, if this is the last item in the list...)
                    if (current->next == NULL) {
                        // create a new item
                        int_list* new_item = malloc(sizeof(int_list));
                        // place the data_item_int in this new item
                        latest_item = col_current->data;
                        while (latest_item->next != NULL)
                            latest_item = latest_item->next;
                        latest_item = new_item;
                        // point the current int_list next to where the new item is to be found
                        current->next = new_item;
                        // ... and set the new item next to null.
                        new_item->next = NULL;
                        // go to the next roster
                        roster = roster->next;
                        col_current = col_current->next_col;
                    }
                    else {
                        // otherwise, traverse the list.
                        current = current->next;
                  };

                //roster = roster->next;
                };
            query_command = linecounter + 1;

            }
        }
        while (!feof(database));
/*        roster = roster_start;
        printf("starting with roster_start = %s.%s.%s\n", roster->db_name, roster->tbl_name, roster->col_name);
        Column* current_col = table_search->columns;
        printf("starting with column %s\n", table_search->columns->name);
        while (roster) {
            printf("looking at col %s\n", current_col->name);
            current_col->data = malloc(sizeof(int_list*));
            current_col->data = roster->items;
            printf("excerpt of column data: %i", current_col->data->item);
            int_list* sample = current_col->data->next;
            printf(" %i ...", sample->item);
            if (roster->next) {
                current_col->next_col->data = malloc(sizeof(int_list*)); 
                current_col->next_col->data = roster->next->items;
            }
            else
                current_col->next_col = NULL;
            current_col = current_col->next_col;
            roster = roster->next;
        };
*/
    }
    else if (strncmp(filetype, "dsl", 3) == 0)
    {
        DbOperator* x;
        if (!feof(database)) do
        {
            fgets(query_command, 100, database);
            x = parse_command(query_command, send_message, client_socket, context, db_head, var_pool);
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
    unsigned int columns_inserted = 0;
    char* token = NULL;
    char* db_name = malloc(100);
    db_name[0] = '\0';
    char* table_name = malloc(100);
    char** command_index;
    // Db *db = NULL;  // assume null for now though will have ot pass db here to parse insert ultimately
    // check for leading '('
    if (query_command[0] == '(') {
        query_command++;
        command_index = &query_command;
        // parse table input
        db_name = next_token(command_index, &send_message->status);
        int last_char = strlen(db_name);
        db_name[last_char] = '\0';
        strcpy(table_name, divide_period(db_name));
        printf("divided db and table = %s and %s\n", db_name, table_name);
        //if (send_message->status == INCORRECT_FORMAT) {
        //    return NULL;
        //}
        // lookup the table and make sure it exists. 
        //Table* insert_table = malloc(sizeof(Table));
        Table* insert_table = malloc(sizeof(Table));
        insert_table = lookup_table(table_name, db_head);
        if (strcmp(insert_table->name, table_name) != 0) {
            printf("couldn't find table so exiting out\n");
            return NULL;
        }

        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        // dbo->operator_fields.insert_operator.table = insert_table;
        // dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer. 

        // create an array of insert values

        int_list* start = malloc(sizeof(int_list));
        int_list* current = start;
        int_list* prev = NULL;
        token = strsep(command_index, ",");
        int insert_val = atoi(token);
        //printf("now inserting %i\n", insert_val);
        current->item = insert_val;
        current->deleted_flag = 0;
        current->next = malloc(sizeof(int_list)); // * or just int_list?
        current->next = NULL;
        while ((token = strsep(command_index, ",")) != NULL) {
            insert_val = atoi(token);
            //printf("now inserting %i\n", insert_val);
            current->next = malloc(sizeof(int_list)); // * or just int_list?
            prev = current;
            current = current->next;
            current->item = insert_val;
            current->deleted_flag = 0;
            current->next = malloc(sizeof(int_list));
            current->next = NULL;
        }
        prev->next = NULL;
        Column* current_column = malloc(sizeof(Column));
        current_column->next_col = malloc(sizeof(Column*));
        current_column = insert_table->columns;
        int modify_column = insert_val;
        printf("Adding to column %i\n", modify_column);
        //free(current);
        for(int x = 1; x < modify_column; x++)
        {
            printf("hopping to col %i\n", x);
            if (current_column->next_col == NULL) {
                printf("out of columns - exiting\n");
                return NULL; }
            current_column = current_column->next_col;
        }
        if (current_column->data == NULL) {
            printf("column is empty; setting it equal to new one just created\n");
            current_column->data = start;
            printf("set.\n");
        } 
        else {
            printf("traversing col to end...\n");
            do {
                current_column->data = current_column->data->next;
                printf("hop over item %i...", current_column->data->item);
            }
            while (current_column->data->next != NULL);
            printf("found end of column, now appending by linking...\n");
            current_column->data->next = start;
        }

        // check that we received the correct number of input values
        // if (columns_inserted != insert_table->col_count) {
        //    printf("wrong number of columns\n");
        //    send_message->status = INCORRECT_FORMAT;
        //    free (dbo);
        //    return NULL; 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
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
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context, Db* db_head, Var* var_pool) {
    DbOperator *dbo = malloc(sizeof(DbOperator)); // calloc?
    printf("query is: %s\n", query_command);
    if (strncmp(query_command, "--", 2) == 0) {
        //printf("This is a comment - nothing to do here!\n");
        //send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;

    } else {
        handle = NULL;
    };

    cs165_log(stdout, "QUERY: %s\n", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    // send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    // check what command is given. 
    //printf("is there a handle?\n");
    if (handle != NULL) {
        //printf("YES, there's a handle. It is %s\n", handle);
        //query_command += strlen(handle) + 1;
        char *open_pren = strchr(query_command, '(');       // find open pren
        if (open_pren == NULL) {
            dbo->type = NA;
            printf("No valid command entered.\n");
            return dbo;
        }
        else {
            *open_pren = '\0';
            printf("now, what to make of command %s...\n", query_command);
            if (strncmp(query_command, "join", 4) == 0) {
                query_command += 5;         // skip over join leters + open pren
                printf("PLACEHOLDER: Insert JOIN logic here\n");
            } else if (strncmp(query_command, "select", 6) == 0) {
                query_command += 7;         // skip over select letters + open pren
                char* period_pointer = strchr(query_command, '.');
                char* comma_pointer;
                char* pren_pointer;
                char* db_name = query_command;
                int low_value, high_value;
                if (period_pointer != NULL) {
                    *period_pointer = '\0';
                    query_command = ++period_pointer;
                };
                printf("tracking down db: %s\n", db_name);
                period_pointer = strchr(query_command, '.');
                char* table_name = query_command;
                if (period_pointer != NULL) {
                    *period_pointer = '\0';
                    query_command = ++period_pointer;
                };
                printf("tracking down table: %s\n", table_name);
                comma_pointer = strchr(query_command, ',');
                char* column_name = query_command;
                if (comma_pointer != NULL) {
                    *comma_pointer = '\0';
                    query_command = ++comma_pointer;
                };
                comma_pointer = strchr(query_command, ',');
                char* low_string = query_command;
                if (comma_pointer != NULL) {
                    *comma_pointer = '\0';
                    low_value = atoi(low_string);
                    query_command = ++comma_pointer;
                };
                printf("low value = %s\n", low_string);
                pren_pointer = strchr(query_command, ')');
                char* high_string = query_command;
                if (pren_pointer != NULL) {
                    *pren_pointer = '\0';
                    high_value = atoi(high_string);
                };
                printf("high_value = %s\n", high_string);
                select_row(db_name, table_name, column_name, low_value, high_value, db_head, handle, var_pool);
                printf("completed select operator\n");
            } else if (strncmp(query_command, "fetch", 5) == 0) {
                query_command += 6;         // skip over fetch letters + open pren
                printf("PLACEHOLDER: Insert FETCH logic here\n");
            }
        }
    } else {
            printf("No handle here.\n");
            if (strncmp(query_command, "create", 6) == 0) {
                query_command += 6;
                //send_message->status = parse_create(query_command, db_head, var_pool);
                parse_create(query_command, db_head, var_pool);
                dbo = malloc(sizeof(DbOperator));
                dbo->type = CREATE;
            } else if (strncmp(query_command, "relational_insert", 17) == 0) {
                query_command += 17;
                dbo = parse_insert(query_command, send_message, db_head);
            } else if (strncmp(query_command, "load", 4) == 0) {
                query_command += 4;
                dbo = load_db(query_command, db_head, var_pool, client_socket, context);
            } else if (strncmp(query_command, "lookup", 6) == 0) {
                query_command += 6;
                message_status mes = parse_lookup(query_command, db_head);
                //dbo->type = LOOKUP;
            } else if (strncmp(query_command, "print_db", 8) == 0) {
                query_command += 8;
                print_db(db_head);
            } else if (strncmp(query_command, "print_var", 9) == 0) {
                query_command += 9;
                print_var(var_pool, query_command);
            } else {
                dbo->type = NA;
                if (dbo->type == NA) {
                    printf("No valid command entered!\n");
                    return dbo;
                };
            }
        }
    //dbo->client_fd = client_socket;
    //dbo->context = context;
    //return dbo;
};
