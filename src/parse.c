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

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
#define _DEFAULT_SOURCE
#define FALSE 0
#define TRUE 1
#define DELETE_VALUE -2147483647
#define SIZE_INT_LIST 1018

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "parse.h"
#include "utils.h"
#include "client_context.h"

char* itoa (int value, char* result, int base)
{
    char *ptr = result, *ptr1 = result, tmp_char;
    int tmp_value;

    do {
        tmp_value = value;
        value /= base;
        *ptr++ = "zyxwvutsrqponmlkjihgfedcba9876543210123456789abcdefghijklmnopqrstuvwxyz" [35 + (tmp_value - value * base)];
    } while (value);

    if (tmp_value < 0) *ptr++ = '-';
    *ptr-- = '\0';
    while (ptr1 < ptr) {
        tmp_char = *ptr;
        *ptr-- = *ptr1;
        *ptr1++ = tmp_char;
    }
    return result;
}

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

// The purpose of this function is simply to determine whether to set the flags in the columns so that
// indexes are formed. The functions that actually *populate* the columns are the ones that create the
// indexes themselves.

message_status parse_create_idx(char* create_arguments, Db* db_head) {
    char* db_name = malloc(100);
    char* table_name = malloc(100);

    printf("In parse_create_idx function\n");
    message_status status = OK_DONE;

    // Find db name, tbl name and col name from args.
    char** create_arguments_index = &create_arguments;
    db_name = next_token(create_arguments_index, &status);
    char* tbl_name = divide_period(db_name);
    char* col_name = divide_period(tbl_name);
    printf("db_name = %s\n", db_name);
    printf("tbl name = %s\n", tbl_name);
    printf("col name = %s\n", col_name);

    // Find index types 1 and 2
    char* index_type_1 = next_token(create_arguments_index, &status);
    printf("index type 1 = %s\n", index_type_1);
    char* index_type_2 = next_token(create_arguments_index, &status);
    printf("index type 2 = %s\n", index_type_2);

    // Get the column name free of quotation marks
    col_name = trim_quotes(col_name);
    printf("Create column with index full name = %s\n", col_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(index_type_2) - 1;
    if (index_type_2[last_char] != ')') {
       return INCORRECT_FORMAT;
    };
    index_type_2[last_char] = '\0';
    strcpy(table_name, tbl_name);

    Db* db_search = malloc(sizeof(Db));
    db_search = lookup_db(db_name, db_head);
    if (strcmp(db_search->name,db_name) != 0) {
        fflush(stdout);
        return QUERY_UNSUPPORTED;
        };

    // find the table in the db
    Table *table_search = malloc(sizeof(Table));
    table_search = lookup_table(table_name, db_head);
    Column* column_search;
    if (strcmp(table_search->name,table_name) != 0) {
        fflush(stdout);
        return QUERY_UNSUPPORTED;
        }
    else
    {
        column_search = lookup_column(col_name, table_search);
        printf("table search name = %s and col name = %s in db_name = %s\n", table_name, column_search->name, db_head->name);
    };
    // set flags according to index type
    if (strcmp(index_type_1, "btree") == 0)
        column_search->btree = 1;
    else
        column_search->btree = 0;

    if (strcmp(index_type_2, "clustered") == 0)
        column_search->clustered = 1;
    else
        column_search->clustered = 0;   

    // add flag to column showing that there IS an index.
    column_search->index_present = 1;
    printf("Set %s to have index %i for btree, %i for clustered", column_search->name, column_search->btree, column_search->clustered);

    return OK_DONE;
}


message_status parse_create_col(char* create_arguments, Db* db_head) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* db_and_table_name = next_token(create_arguments_index, &status);

    // Get the column name free of quotation marks
    col_name = trim_quotes(col_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(db_and_table_name) - 1;
    if (db_and_table_name[last_char] != ')') {
       return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    db_and_table_name[last_char] = '\0';

    // isolate table name
    char* table_name = malloc(100);
    strcpy(table_name, divide_period(db_and_table_name));
    // isolate db name
    char* db_name = malloc(100);
    strcpy(db_name, db_and_table_name);

    // Does Db exist? If not, exit out
    Db* db_search = malloc(sizeof(Db));
    db_search = lookup_db(db_name, db_head);
    if (strcmp(db_search->name,db_name) != 0) 
        return QUERY_UNSUPPORTED;

    // find the table in the db
    Table *table_search = malloc(sizeof(Table));
    table_search = lookup_table(table_name, db_head);
    if (strcmp(table_search->name,table_name) != 0)
        return QUERY_UNSUPPORTED;
    else
        create_column(table_search, col_name, 0);
}

message_status parse_create_tbl(char* create_arguments, Db* db_head) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

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
    Db* db_search = malloc(sizeof(Db));
    db_search = lookup_db(db_name, db_head);
    if (!db_search || !db_search->name) {
        Var* var_pool = malloc(sizeof(Var));
        db_search = create_db(db_name, db_head, var_pool);
        }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Table* create_status = malloc(sizeof(Table));
    //printf("going into lookup_table with table name %s and db_search %s\n", table_name, db_search->name);
    //bool found = malloc(sizeof(bool)); 
    //found = 0;
    create_status = lookup_table(table_name, db_search);

    if (create_status == NULL) 
        create_table(db_search, table_name, column_cnt, 0);

    return OK_DONE;
}

void shutdown(Db* db_head) {
    char buffer[100];
    char cwd[100];
    char* itoa_buffer = malloc(20);
    char db_name[100], col_name[100];


    printf("Shutting down db and committing to disk.\n");
    if (db_head == NULL) {
        return;
    }
    else {
        Db* current_db = db_head;
        do
        {
            printf("Database %s:\n", current_db->name);
            db_name[0] = '\0';
            strcat(db_name, current_db->name);
            strcat(db_name, ".dsl");
            FILE* database = fopen(db_name, "w");
            buffer[0] = '\0';
            strcat(buffer, "create(db,");
            strcat(buffer, current_db->name);
            strcat(buffer, ")\n");
            fprintf(database, "%s", buffer);

            if (current_db->tables == NULL) printf("  Tables list is empty.\n");
            else {
                Table* current_table = current_db->tables;
                do
                {
                    //Table* current_table = current_db->tables;
                    printf("  Table %s:\n", current_table->name);
                    buffer[0] = '\0';
                    strcat(buffer, "create(tbl,");
                    strcat(buffer, current_table->name);
                    strcat(buffer, ",");
                    strcat(buffer, current_db->name);
                    strcat(buffer, ",");
                    strcat(buffer, itoa(current_table->col_count, itoa_buffer, 10));
                    strcat(buffer, ")\n");
                    fprintf(database, "%s", buffer);
                    if (current_table->columns == NULL) 
                        printf("    Column list is empty.\n");
                    else {
                        Column* current_col = current_table->columns;
                        int col_number = 1;
                        do
                        {
                            col_name[0] = '\0';
                            strcat(col_name, current_table->name);
                            strcat(col_name, "_");
                            strcat(col_name, current_col->name);
                            strcat(col_name, ".csv");
                            FILE* column_file = fopen(col_name, "w");
                            printf("    Column %s (%i)\n", current_col->name, col_number++);
                            buffer[0] = '\0';
                            strcat(buffer, "create(col,");
                            strcat(buffer, current_col->name);
                            strcat(buffer, ",");                            
                            strcat(buffer, current_db->name);
                            strcat(buffer, ".");
                            strcat(buffer, current_table->name);
                            strcat(buffer, ")\n");
                            fprintf(database, "%s", buffer);

                            buffer[0] = '\0';
                            strcat(buffer, "load(");
                            getcwd(cwd, sizeof(cwd));
                            strcat(buffer, cwd);
                            strcat(buffer, "/");
                            strcat(buffer, current_table->name);
                            strcat(buffer, "_");
                            strcat(buffer, current_col->name);
                            strcat(buffer, ".csv)\n");
                            fprintf(database, "%s", buffer);

                            buffer[0] = '\0';
                            strcat(buffer, current_db->name);
                            strcat(buffer, ".");
                            strcat(buffer, current_table->name);
                            strcat(buffer, ".");
                            strcat(buffer, current_col->name);
                            strcat(buffer, "\n");
                            fprintf(column_file, "%s", buffer);

                            if (current_col->data == NULL) printf("      No data in this column.\n");
                            else {
                                printf("    ");
                                int_list* current_data = current_col->data;
                                do {
                                    for (unsigned int x = 0; x < current_data->count; x++) {
                                        itoa(current_data->item[x], itoa_buffer, 10);
                                        fprintf(column_file, "%s", itoa_buffer);
                                        fprintf(column_file, "\n");
                                    }
                                    current_data = current_data->next;
                                }
                                while (current_data != NULL);
                                fclose(column_file);
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
            fclose(database);
        }
        while (current_db != NULL);
        
    }
}


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
                    if (current_table->columns == NULL) 
                        printf("    Column list is empty.\n");
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
                                do {
                                    for (unsigned int x = 0; x < current_data->count; x++)
                                    {
                                        if (current_data->item[x] != DELETE_VALUE)
                                            printf(" %i ", current_data->item[x]);
                                    };
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
                mes_status = parse_lookup_db(tokenizer_copy, db_head);
            } else if (strcmp(token, "tbl") == 0) {
                mes_status = parse_lookup_tbl(tokenizer_copy, db_head);
            } else if (strcmp(token, "col") == 0) {
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
                //printf("Been asked to create a db\n");
                mes_status = parse_create_db(tokenizer_copy, db_head, var_pool);
            } else if (strcmp(token, "tbl") == 0) {
                //printf("Been asked to create a tbl\n");
                mes_status = parse_create_tbl(tokenizer_copy, db_head);
            } else if (strcmp(token, "idx") == 0) {
                //printf("Been asked to create an index\n");
                mes_status = parse_create_idx(tokenizer_copy, db_head);
            } else if (strcmp(token, "col") == 0) {
                //printf("Been asked to create a col\n");
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

DbOperator* load_db(char* dbFile, Db* db_head, Var* var_pool, int client_socket, struct ClientContext* context, int* batch_mode, DbTblCol* batch) {
    char* query_command = malloc(100);
    char* query_command_backup = query_command;
    char* start_counter;
    message *send_message;

    // PARSING SECTION

    // skip prens and quotes
    if (dbFile[0] == '(')
        dbFile++;
    int last_char = strlen(dbFile);
        dbFile[last_char - 1] = '\0';
    dbFile = trim_quotes(dbFile);
    //printf("opening %s\n", dbFile);


    // open the file for reading and writing
    FILE* database = fopen(dbFile, "r");
    if (database == NULL) {
        return NULL;
    };

    // There are two styles of files that can be loaded, .csv and .dsl. They are treated differently.
    // Logic: Is it a csv file or a dsl file? If not, return an error.
    char filetype[4], db_name[100], tbl_name[100], col_name[100];

    // Find suffix
    strcpy(filetype, dbFile + strlen(dbFile) - 3);
    char* linecounter;
    int end_of_line = 0;

    if (strncmp(filetype, "dsl", 3) == 0)
    {
    // repeatedly scan and execute until the end of file is reached.
        if (!feof(database)) do
        {
            fgets(query_command, 100, database);
            parse_command(query_command, send_message, client_socket, context, db_head, var_pool, batch_mode, batch);
        }
        while (!feof(database));
    };

    // Is it csv? If so ...
    if (strncmp(filetype, "csv", 3) == 0)
    {
    // The first line of the file defines the db name / tables / columns, and it's important to know
    // these because the data on each line is shipped to different columns when there's more than one
    // db/tbl/col triptych listed.
        DbTblCol* roster = malloc(sizeof(DbTblCol));
        roster->items = malloc(sizeof(int_list*));
        // 1/26 11:41 roster->items = NULL;
        DbTblCol* roster_start = roster;
        // read a line of data, store in query_command
        fgets(query_command, 100, database);
        //printf("read first line: %s\n", query_command);
        while (!end_of_line) {
            // text before the first period is the db name...
            linecounter = strchr(query_command, '.');
            *linecounter = '\0';
            strcpy(roster->db_name, query_command);
            //printf("db name is %s  ", roster->db_name);

            // text before the next period is the tbl name...
            query_command = linecounter + 1;
            linecounter = strchr(query_command, '.');
            start_counter = query_command;
            *linecounter = '\0';
            query_command = (linecounter + 1);
            strcpy(roster->tbl_name, start_counter);
            //printf("table name is %s  ", roster->tbl_name);

            // text before the comma is the col name...
            linecounter = strchr(query_command, ',');
            col_name[strlen(col_name)] = '\0';

            // ... but there may not BE a comma, if it's the last one in the line.
            if (linecounter == NULL)
            {
                strcpy(roster->col_name, query_command);
                //printf("col name is %s\n", roster->col_name);
                // ends the while by signaling the last item in the line to process.
                end_of_line = 1;      
                roster->next = NULL;
            }
            else {
                *linecounter = '\0';
                query_command = trim_whitespace(query_command);
                strcpy(roster->col_name, query_command);
                //printf("col name is %s\n", roster->col_name);
                // since there's a comma, there's going to need to be a new roster item, so
                // the following lines create and link it.
                query_command = linecounter + 1;
                DbTblCol* new_roster = malloc(sizeof(DbTblCol));
                roster->next = new_roster;
                roster = new_roster;
            };
        };
        // we don't want to lose the roster, so we reset the pointer to the backup copy, roster_start.
        roster = roster_start;

        // Now that the list of db/tbl/cols is done, we set up the columns so we can populate.
        // And the first step to doing that is: find the pointer to the table where they are supposed
        // to be housed.
        Table *insert_table = malloc(sizeof(Table*));               // create an insert table pointer object
        //printf("looking for table %s\n", roster->tbl_name);
        insert_table = lookup_table(roster->tbl_name, db_head);     // look up the table name in the roster
        //printf("table pointer = %p\n", insert_table);
        // Q: Does the call to lookup_table CREATE the table if it doesn't exist?
        // A: It does.

        // Not sure this code is really helpful - it just creates a blank column but doesn't
        // populate it - may be better to build a SET of columns or traverse looking for the right
        // column and create the one requested if it's not there.
        if (insert_table->columns == NULL) {
            DbTblCol* roster_working = roster;
            Table* insert_table_working = insert_table;
            Column* first_column = NULL;
            while (roster_working != NULL) {
                Column* new_column = malloc(sizeof(Column));
                if (first_column == NULL)
                    first_column = new_column;
                strcpy(new_column->name, roster->col_name);
                roster_working = roster_working->next;
            };
            insert_table->columns = first_column;
        };

        query_command = query_command_backup;                       // point the query command pointer to the original (since it's moved during the line scan process)
        int insert_val;                                             // this is the integer representation of the numbers read off the csv file

        int_list* start = malloc(sizeof(int_list*));                                        // declare start pointer for int_list
        int_list* current = malloc(sizeof(int_list*));                                      // declare current pointer for int_list
        current = start;
        Column* insert_column;

        if (!feof(database)) do                                      // while eof hasn't been reached...
        {

            *query_command = '\0';
            fgets(query_command, 100, database);                    // read a new line from the file, place in query_command after some pre-processing
            query_command = trim_whitespace(query_command);         // trim any excess whitespace            
            if (*query_command == '\0') break;
            //printf("new line: %s\n", query_command);

            roster = roster_start;                                  // go to the beginning of the roster of db/table/column inserts
            insert_column = insert_table->columns;                  // Now create an insert column pointer and point it to insret_table's first column
            //if (insert_column->index != NULL)
            //    printf("%s col has index!\n", insert_column->name);
            int index_array_max_size, current_index_size;
            int* index_array;
            do {                                                    // begin loop
                if (insert_column->data == NULL) {
                    int_list* new_block = malloc(sizeof(int_list)); // Now create an int_list node pointer called start
                    current = new_block;
                    current->count = 0;     
                    insert_column->data = current;
                };
                query_command[strcspn(query_command, ",")] = '\0';  // isolate next value to insert
                insert_val = atoi(query_command);                       
                //printf("v: %i\n", insert_val);                          

                //printf("storing %i at int_list item #%i in %s\n", insert_val, current->count, insert_column->name);
                current->item[current->count] = insert_val;                             // store the value
                current->count++;

                // here is the index logic.

                int current_index_size, index_array_max_size;
                int* index_array;

                if (insert_column->btree == 0 && insert_column->index_present == 1) {   
                    // create a new index object if currently there is none, initialize working variables
                    if (insert_column->index == NULL) {                 
                        //printf("no pre-existing index object so creating\n");
                        // max size assumed to be 1000 to start off with.
                        index_array_max_size = 1000;
                        current_index_size = 0;
                        // create integer array and point to it in the column object
                        index_array = malloc(sizeof(int)*index_array_max_size);
                        insert_column->index = index_array;
                    };
                    index_array[current_index_size] = current_index_size++; // add one index item
                    // if index size exceeds capacity, realloc times two.
                    if (current_index_size > index_array_max_size) {
                        //printf("expanding index size from %i to %i\n", index_array_max_size, index_array_max_size*2);
                        index_array_max_size *= 2;
                        index_array = realloc(index_array,index_array_max_size * sizeof(int));
                    };
                }
                else if (insert_column->btree == 1 && insert_column->index_present == 1) {
                    //printf("initializing btree index");
                    if (insert_column->index == NULL) {
                        //printf("no pre-existing btree object so creating\n");
                        node* rootnode = malloc(sizeof(node));
                        node* reserve_node = malloc(sizeof(node));
                        fence_pointer* fence = malloc(sizeof(fence_pointer));
                        int* occupancy_matrix_ptr = malloc(sizeof(int));
                        insert_column->index = initialize_btree(rootnode, reserve_node, fence, occupancy_matrix_ptr);
                    };
                };
                // end of index logic.

                if (current->count == SIZE_INT_LIST)                                {
                    int_list* new_block = malloc(sizeof(int_list));
                    current->next = new_block;                      // point current to newly created int list
                    current = new_block;                            // now the new block IS current
                    current->count = 0;
                };

                query_command += strcspn(query_command, ",") + 1;       // move the query command pointer to just after the null terminator

                if (roster->next) {
                    roster = roster->next;
                    insert_column = lookup_column(roster->col_name, insert_table);
                    //printf("looking up column %s\n", insert_column);
                    current = insert_column->data;
                }
                else {
                    roster = roster_start;
                    insert_column = lookup_column(roster->col_name, insert_table);
                    //printf("looking up column %s\n", insert_column);
                    current = insert_column->data;
                };
            } while (strcspn(query_command, ",") != 0);
        query_command = query_command_backup;
        roster = roster_start;
        } while (!feof(database));

        // Now go through and sort the sorted columns indexes and reshuffle.

        insert_column = insert_table->columns;
        int total_item_count;

        // Ordinarily the b-tree is supposed to have separate logic but I was not fully able to implement, so wanted this
        // to still run at least... so commented that code out.

        if (//insert_column->btree == 0 && //
            insert_column->index_present == 1 //&& insert_column->clustered == 1
            ) {
            // load entire column into array
            //printf("sorting column to create index.\n");
            //printf("loading it into an array --\n");
            total_item_count = 0;
            int_list* working_data = insert_column->data;
            while (working_data != NULL) {
                total_item_count += working_data->count;
                printf("running total...");
                working_data = working_data->next;
            };
            printf("total item count to sort: %i\n", total_item_count);

            int total_column_array[total_item_count];
            int total_index_array[total_item_count];

            working_data = insert_column->data;
            int x_index = 0;
            while (working_data != NULL) {
                for (unsigned int x = 0; x < working_data->count; x++) {
                    total_column_array[x_index + x] = working_data->item[x];
                    total_index_array[x_index + x] = x_index + x;
                };
                x_index += working_data->count;
                working_data = working_data->next;
            };
            //printf("Here the total array: \n");
            //for (int y = 0; y < total_item_count; y++)
            //    printf("%i : %i  ", y, total_column_array[y]);

            // send column and associated index into quicksort
            int min_item = 2147483647;
            int max_item = -2147483647;
            for (int y = 0; y < total_item_count; y++) {
                min_item = MIN(min_item, total_column_array[y]);
                max_item = MAX(max_item, total_column_array[y]);
            };
            printf("executing the quicksort...\n");
            quicksort(&total_column_array, &total_index_array, min_item, max_item);
            //printf("Here the total array: \n");
            //for (int y = 0; y < total_item_count; y++)
            //    printf("%i : %i  ", y, total_column_array[y]);

                // chop back up until int_lists
            int_list* new_block = malloc(sizeof(int_list));
            new_block->count = 0;
            int_list* old_block = NULL;
            int_list* next_reserve = insert_column->next_col;
            insert_column->data = new_block;
            for (int y = 0; y < total_item_count; y+= SIZE_INT_LIST) {
                for (int x = 0; x < MIN(SIZE_INT_LIST, total_item_count - y); x++) {
                    new_block->item[y + x] = total_column_array[y + x];
                    new_block->count++;
                };
                if (old_block == NULL) old_block = new_block;
                new_block = malloc(sizeof(int_list));
                old_block->next = new_block;
            };
            insert_column->data = old_block;
            insert_column->next_col = next_reserve;

            // NOW SORT THE OTHER COLUMNS ACCORDING TO THE INDEX!

            //for (int y = 0; y < total_item_count; y++)
            //    printf("%i ", total_index_array[y]);
            //printf("\n");

            Column* start_column = insert_column;

            // loop through the columns...
            while (start_column != NULL) {

                // skip over the column that IS the index, because that would just mess up the brand new order...
                if (start_column->btree == 0 && start_column->index_present == 1 && insert_column->clustered == 1) {
                    printf("skipping col %s\n", start_column->name);
                    start_column = start_column->next_col;
                }

                //if (start_column->btree == 0 && start_column->index_present == 1 && insert_column->clustered == 1)
                //    start_column = start_column->next_col;
                else {
                    // declare an array
                    int sorting_array[total_item_count];
                    
                    int_list* source_data = start_column->data;
                    int tuple = 0;
                    while (start_column != NULL) {
                        // place the contents of each column into the array and sort according to index
                        while (source_data != NULL) {
                            for (int z = 0; z < source_data->count; z++)
                                sorting_array[tuple++] = source_data->item[total_index_array[z]];
                            source_data = source_data->next;
                        };
                            printf("Newly sorted column %s:", start_column->name);
                            for (int g = 0; g < total_item_count; g++)
                                printf("%i ", sorting_array[g]);
                            printf("\n\n");
                            // Now put the re-ordered contents into int_lists...
                            if (start_column) {
                                start_column->data->count = 0;
                                for (int g = 0; g < total_item_count; g++) {
                                    start_column->data->item[start_column->data->count] = sorting_array[g];
                                    start_column->data->count++;
                                    if (start_column->data->count == SIZE_INT_LIST) {
                                        start_column->data = start_column->data->next;
                                        start_column->data->count = 0;
                                    };
                                };
                            };
                            printf("\n\nmoving from %s ", start_column->name);
                            start_column = start_column->next_col;
                            printf("%s", start_column->name);

                    };

                    };
                };
            };
        };

 /*       else if (insert_column->btree == 1 && insert_column->index_present == 1 && insert_column->clustered == 1) {
        // insert b-tree logic here
            int_list* working_data = insert_column->data;
            int x_index = 0;
            node* rootnode = insert_column->index;
 
            while (working_data != NULL) {
                for (int x = 0; x < working_data->count; x++) {
                    btree_put(rootnode, working_data->item[x]);
                };
                working_data = working_data->next;
            };
    }; */
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

    //printf("Reading db completed.\n");
    fclose(database);
    //printf("FINISHED LOAD\n");
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
        //printf("divided db and table = %s and %s\n", db_name, table_name);

        //
        //query_command += last_char + 2;
        //printf("start of variable list = %s", query_command);
        //

        // establish table pointer and lookup table listed
        Table* insert_table = malloc(sizeof(Table*));               // declare a table pointer called insert_table 
        bool found = malloc(sizeof(bool));
        insert_table = lookup_table(table_name, db_head);    // lookup the table by name and place the pointer in insert_table    

        // check to make sure table actually exists, exit if no.
        if (insert_table == NULL) {
            //printf("couldn't find table so exiting out\n");
            return NULL;
        }

        Column* insert_column = insert_table->columns;              // Now create an insert column pointer and point it to insret_table's first column 

        int_list* start = malloc(sizeof(int_list*));                // Now create an int_list node pointer called start
        int_list* current = start;
        while (strcspn(query_command, ",") != 0)                    // as long as there is a comma to be found in query command...
        {

            query_command[strcspn(query_command, ",")] = '\0';      // change the comma to a null terminator
            //char* next_char = strsep(query_command, '\0') + 1;
            insert_val = atoi(query_command);                       // collect the val preceding the null terminator and change to an int, store in insert_val
            query_command += strcspn(query_command, ",") + 1;       // move the query command pointer to just after the null terminator
            //printf("interpreted atoi result as %i\n", insert_val);  // display the value just read

            start = insert_column->data;
            if (start == NULL) {
                int_list* new_int = malloc(sizeof(int_list));           // now create a new int_list object to store it in
                new_int->count = 0;
                insert_column->data = new_int;
                start = insert_column->data;
            }
            else while (start && start->count == SIZE_INT_LIST)
                start = start->next;

            start->item[start->count++] = insert_val;                             // store the value
            if (start->count == SIZE_INT_LIST) {
                int_list* new_int = malloc(sizeof(int_list));
                new_int->count = 0;
                start = new_int;

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

int_list* parse_delete(char* query_command, char* handle, Db* db_head, Var* var_pool) {
    char* period_pointer = strchr(query_command, '.');
    char* comma_pointer;
    char* pren_pointer;
    char* db_name = query_command;
    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };
    //printf("tracking down db: %s\n", db_name);
    period_pointer = strchr(query_command, '.');
    char* table_name = query_command;
    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };
    //printf("tracking down table: %s\n", table_name);
    comma_pointer = strchr(query_command, ',');
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
    //printf("variable name for delete indexes = %s\n", var_name);
    pren_pointer = strchr(query_command, ')');
    int_list* deleted = delete_row(db_name, db_head, table_name, var_name, var_pool);
    return deleted;
}

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
    //printf("tracking down db: %s\n", db_name);
    period_pointer = strchr(query_command, '.');
    char* table_name = query_command;
    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };
    //printf("tracking down table: %s\n", table_name);
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
    //printf("variable name for fetch indexes = %s\n", var_name);
    pren_pointer = strchr(query_command, ')');
    //printf("Assigning to handle: %s\n", handle);
    int_list* fetch_list = fetch_row(db_name, table_name, column_name, var_name, db_head, handle, var_pool);
    return fetch_list;
}

// The purpose of this function is to accept a series of select queries while the server is in batching mode
// before execution has been requested. What it accepts is the head of the batch_start linked list (which is a
// set of db, tbl and col names along with the handle and high / low as part of the select query), as well as the
// latest query command. What it does is separates out the components of the query command into all the pieces and
// then traverses the DbTblCol list starting at the head (batch_start) and then adds a new db/tbl/col set if needed,
// but otherwise creates a sorted link (by low value). The idea is that when the batch is ultimately executed, it
// READS the data from the page once, and then then populates the result of the select query for each item in the
// batch, where relevant, because it is quicker to change contexts between those variables than it is to read the
// same segment of disk twice.
//
// The exception to the last statement is a TLB miss, and in that case what should happen is there should simply be
// a max on the number of batches it will try to execute at one time.

DbTblCol* parse_select_batch(DbTblCol* batch_start, char* query_command, Db* db_head, Var* var_pool, char handle[100]) {

    // point working variable of DbTblCol type to existing batch
    DbTblCol* batch_working = batch_start;

    // create new DbTblCol object
    DbTblCol* new_batch_object = malloc(sizeof(DbTblCol));

    char* period_pointer = strchr(query_command, '.');
    char* comma_pointer;
    char* pren_pointer;
    char* db_name = query_command;
    //char handle[100];

    strcpy(new_batch_object->db_name, db_name);
    printf("In parse_select_batch: %s is query command\n", query_command);
    printf("Also, %s is batch db\n", batch_start->db_name);

    int low_value, high_value;
    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };

    // find db, tbl and col variables, populate a new batch_item
    //printf("tracking down db: %s\n", db_name);
    period_pointer = strchr(query_command, '.');
    char* table_name = query_command;
    strcpy(new_batch_object->tbl_name, table_name);
    printf("new batch table name is %s\n", new_batch_object->tbl_name);

    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };
    //printf("tracking down table: %s\n", table_name);
    comma_pointer = strchr(query_command, ',');
    char* column_name = query_command;
    if (comma_pointer != NULL) 
    {
        *comma_pointer = '\0';
        query_command = ++comma_pointer;
    };
    strcpy(new_batch_object->col_name, column_name);
    printf("new batch col is %s\n", new_batch_object->col_name);

    comma_pointer = strchr(query_command, ',');
    char* low_string = query_command;
    if (comma_pointer != NULL) 
    {
        *comma_pointer = '\0';
        if (strcmp(low_string, "null") == 0)
            low_value = -2147483646;
        else
            low_value = atoi(low_string);
        query_command = ++comma_pointer;
    };
    new_batch_object->low = low_value;

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
    new_batch_object->high = high_value;

    printf("high_value = %s\n", high_string);
    printf("Assigning to handle: %s\n", handle);
    strcpy(new_batch_object->handle, handle);

    // Now to connect the new batch object into the batch list. Traverse the existing list to determine where it belongs.
    // The list should be in ascending order by low value. Start at the head of the list - if the head of the list has a
    // higher low value, the new object becomes the head of the list. If not then get the next object in the pre-existing list
    // (if it exists) and compare. If new object low is earlier, connect current object to it and connect new object next pointer
    // to the next object. If the end of the list is reached with no object lower, append the list with the new object and then
    // make the next pointer in the new object null.

    printf("placing new item in the batch list...\n");
    if (batch_working == NULL) {
        batch_working = new_batch_object;
        //batch_start = malloc(sizeof(DbTblCol));
        batch_start = new_batch_object;
        batch_start->next = NULL;
        printf("added new item to batch list, addr = %i\n", batch_start);
    } else if (new_batch_object->low <= batch_working->low) {
        new_batch_object->next = batch_working;
        batch_start = new_batch_object;
        batch_working = new_batch_object;
    } else {
        while (batch_working != NULL) {
            printf("Traversing: Current item = %p\n", batch_working);
            DbTblCol* batch_working_test = batch_working->next;
            if (batch_working->next != NULL) {
                if (new_batch_object->low <= batch_working->next->low) 
                {
                    printf("Setting new batch list item to head of list\n");
                    new_batch_object->next = batch_working->next;
                    batch_working->next = new_batch_object;
                    return new_batch_object;
                }
                else batch_working = batch_working_test;
            }
            else {
                batch_working->next = new_batch_object;
                return batch_start;
            };
        };
    };

//    int_list* selection = select_row(db_name, table_name, column_name, low_value, high_value, db_head, handle, var_pool);
//    return selection;
return batch_start;
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
    //printf("tracking down db: %s\n", db_name);
    period_pointer = strchr(query_command, '.');
    char* table_name = query_command;
    if (period_pointer != NULL) 
    {
        *period_pointer = '\0';
        query_command = ++period_pointer;
    };
    //printf("tracking down table: %s\n", table_name);
    comma_pointer = strchr(query_command, ',');
    char* column_name = query_command;
    if (comma_pointer != NULL) 
    {
        *comma_pointer = '\0';
        query_command = ++comma_pointer;
    };
    comma_pointer = strchr(query_command, ',');
    //printf("tracking down table: %s\n", table_name);

    char* low_string = query_command;
    if (comma_pointer != NULL) 
    {
        *comma_pointer = '\0';
        if (strcmp(low_string, "null") == 0)
            low_value = -2147483646;
        else
            low_value = atoi(low_string);
        query_command = ++comma_pointer;
    };
    //printf("low value = %s\n", low_string);
    //printf("tracking down table: %s\n", table_name);

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

    int_list* selection = select_row(db_name, table_name, column_name, low_value, high_value, db_head, handle, var_pool);
    return selection;
}

DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context, Db* db_head, 
    Var* var_pool, int* batch_mode, DbTblCol* batch) 
{
    int_list* result = malloc(sizeof(int_list));
    DbOperator *dbo = malloc(sizeof(DbOperator)); 

    //printf("\n\nQUERY IS: %s\n", query_command);

    // if leads with -- is a comment and should be ignored
    if (strncmp(query_command, "--", 2) == 0) 
        return NULL;

    if (strncmp(query_command, "batch_execute()", 15) == 0) {
        if (*batch_mode == 0) {
            printf("not currently in batching mode; cannot execute a batch because none exists!\n");
            return NULL;
        };
        *batch_mode = 0;
        while(batch != NULL) {
            printf("Executing batched commands now\n");
            printf("Executing batch command: sel %s.%s.%s low %i high %i\n", *batch->db_name, *batch->tbl_name, 
                *batch->col_name, batch->low, batch->high);
            select_row_batch(batch, db_head, var_pool);
            //DbTblCol* batch_old = batch;
            batch = batch->next;
            //free(batch_old);
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
        //cs165_log(stdout, "FILE HANDLE: %s\n", handle);
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
            load_db(query_command, db_head, var_pool, client_socket, context, batch_mode, batch);
        }; 
        if (strncmp(query_command, "lookup", 6) == 0) 
        {
            query_command += 6;
            message_status mes = parse_lookup(query_command, db_head);
        }; 
        if (strncmp(query_command, "print_db", 8) == 0) 
        {
            query_command += 8;
            print_db(db_head);
        }; 
        if (strncmp(query_command, "relational_delete", 17) == 0) 
        {
            query_command += 18;         // skip over fetch letters + open pren
            result = parse_delete(query_command, handle, db_head, var_pool);
        }; 
        if (strncmp(query_command, "shutdown", 8) == 0) 
        {
            query_command += 8;
            shutdown(db_head);
        }; 
        if (strncmp(query_command, "print", 5) == 0) 
        {
            query_command += 5;
            print_var(var_pool, query_command, client_socket);
        }; 
        if (strncmp(query_command, "batch_queries()", 15) == 0)
            {
                printf("Requested to enter batch mode\n");
                if (*batch_mode == 1) {
                    printf("Already in batch mode - cannot re-enter until batch_execute has occurred.\n");
                }
                else {
                    printf("Apologies, but this feature is under construction. /-:");
                    //*batch_mode = 1;
                    //batch = (DbTblCol*)malloc(sizeof(DbTblCol));
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
                char r1_name[100], r2_name[100];

                query_command += 5;         // skip over join leters + open pren
                printf("Executing a join\n");
                int_list* r1 = malloc(sizeof(int_list));
                int_list* r2 = malloc(sizeof(int_list));
                char* comma = strchr(handle, ',');
                char* next_handle = comma + 1;
                *comma = '\0';
                strcpy(r1_name, handle);
                printf("r1 name = %s\n", r1_name);
                printf("next handle = %s\n", next_handle);
                //char* equals = strchr(next_handle, '=');
                //*equals = '\0';
                strcpy(r2_name, next_handle);

                message_status status = OK_DONE;
                char* create_arguments = query_command;
                char** create_arguments_index = &create_arguments;
                char* pos_1 = next_token(create_arguments_index, &status);
                printf("pos_1 = %s\n", pos_1);
                int_list* pos_1_list = interpret_col_or_var (pos_1, var_pool, db_head);

                char* val_1 = next_token(create_arguments_index, &status);
                printf("val_1 = %s\n", val_1);
                int_list* val_1_list = interpret_col_or_var (val_1, var_pool, db_head);

                char* pos_2 = next_token(create_arguments_index, &status);
                printf("pos_2 = %s\n", pos_2);
                int_list* pos_2_list = interpret_col_or_var (pos_2, var_pool, db_head);

                char* val_2 = next_token(create_arguments_index, &status);
                printf("val_2 = %s\n", val_2);
                int_list* val_2_list = interpret_col_or_var (val_2, var_pool, db_head);

                char* join_type = next_token(create_arguments_index, &status);
                trim_parenthesis(join_type);
                int join_type_int;
                if (strcmp(join_type, "nested-loop") == 0) join_type_int = 0;
                if (strcmp(join_type, "hash") == 0) join_type_int = 1;

                printf("type = %s\n", join_type);

                join(db_head, var_pool, r1, r2, pos_1_list, val_1_list, pos_2_list, val_2_list, join_type_int, r1_name, r2_name);

            };
            //printf("testing: is select in command %s...\n", query_command);
            if (strncmp(query_command, "select", 6) == 0) 
            {
                query_command += 7;         // skip over select letters + open pren
                if (*batch_mode == 0) {
                    result = parse_select(query_command, handle, db_head, var_pool);
                    declare_handle(handle, result, var_pool);
                }
                else {
                    printf("batch going into parse_select_batch fxn: %p\n", batch);
                    //batch_start = parse_select_batch(batch_start, query_command, db_head, var_pool, handle);
                    //DbTblCol* batch_2;
                    //batch_2 = parse_select_batch(batch, query_command, db_head, var_pool, handle);
                    parse_select_batch(batch, query_command, db_head, var_pool, handle);
                    //batch = batch_2;
                    // note - don't declare handle until execution.
                    // actual selects are run when executed.
                    printf("batch coming out parse_select_batch fxn: %p\n", batch);
                };
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
                char* comma_pointer = strchr(query_command, ',');
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
                        result->item[0] = find_min(query_command, 0, db_head, var_pool);
                        result->count = 1;
                    printf("Entering batch mode. No further activity - except shutdown - will occur until batch_execute occurs.\n");
                        result->next = NULL;
                        declare_handle(handle, result, var_pool);
                        printf("min is %i\n", result->item[0]);
                    };
                }
                else 
                {
                    char* close_pren = strchr(query_command, ")");
                    if (close_pren == NULL) 
                    {
                        //printf("No close pren - Improper format for min command\n");
                        dbo->type = NA;
                        return dbo;
                    };
                    //printf("now analyzing min\n");
                    int_list* result = malloc(sizeof(int_list));
                    result->item[0] = find_min(query_command, comma_pointer + 1, db_head, var_pool);
                    result->count = 1;
                    declare_handle(handle, result, var_pool);
                    log_info("%i\n", result->item[0]);
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
                        //printf("Improper format for max command\n");
                        dbo->type = NA;
                        return dbo;
                    }
                    else 
                    {
                        int_list* result = malloc(sizeof(int_list));
                        result->item[0] = find_max(query_command, NULL, db_head, var_pool);
                        result->count = 1;
                        result->next = NULL;
                        declare_handle(handle, result, var_pool);
                    };
                }
                else 
                {
                    char* close_pren = strchr(query_command, ')');
                    if (close_pren == NULL) 
                    {
                        //printf("Improper format for max command\n");
                        dbo->type = NA;
                        return dbo;
                    };
                    int_list* result = malloc(sizeof(int_list));
                    result->item[0] = find_max(query_command, comma_pointer + 1, db_head, var_pool);
                    result->count = 1;
                    declare_handle(handle, result, var_pool);
                    log_info("%i\n", result->item[0]);
                };
            };
            if (strncmp(query_command, "sum", 3) == 0) 
            {
                query_command += 4;
                char* close_pren = strchr(query_command, ')');
                if (close_pren == NULL) 
                {
                    //printf("Improper format for sum command\n");
                    dbo->type = NA;
                    return dbo;
                }
                else 
                {
                    int_list* result = malloc(sizeof(int_list));
                    result->next = NULL;
                    result->item[0] = find_sum(query_command, db_head, var_pool);
                    result->count = 1;
                    declare_handle(handle, result, var_pool);
                    log_info("%i\n", result->item[0]);
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
                    float avg = find_avg(query_command, db_head, var_pool);
                    result->count = 1;
                    result->item[0] = avg;
                    //printf("AVG is %f.2\n", avg);
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



