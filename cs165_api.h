

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include "parse.h"
#include "message.h"

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the structf.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
     INT,
     LONG,
     FLOAT,
} DataType;

// DK: The purpose of the following structure is to enable a dynamic length list of inputs for insertion operator.
typedef struct int_list {
    int item;
    bool deleted_flag;          // the deleted flag defaults to FALSE but set to TRUE if has been deleted. Deletes don't REALLY delete.
    struct int_list *next;
} int_list;


//typedef struct Comparator;              // What is this doing here??
//struct ColumnIndex;

typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    struct int_list* data;                 // DK: substituted int_list for int to enable pointers.
    // You will implement column indexes later. 
    void* index;
    //struct ColumnIndex *index;
    //bool clustered;
    struct Column *next_col;               // DK: this is a pointer to the next column over to facilitate search
} Column;


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    struct Column *columns;
    size_t col_count;
    size_t table_length;
    struct Table *next_tbl;            // including this pointer so that we can traverse over different tables.
    int empty_flag;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME]; 
    struct Table *tables;
    size_t tables_size;
    size_t tables_capacity;
    struct Db *next_db;                // including this pointer so that we can have more than one db open at once.
    int empty_flag;
} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  OK,                             /* The operation completed successfully */
  ERROR,                          /* There was an error with the call. */
} StatusCode;

typedef enum JoinType {
    HASH,
    SORTMERGE,
} JoinType;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN,
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

typedef struct GeneralizedColumnList {
    GeneralizedColumnPointer gen_pointer;       // This IS a pointer; doesn't need to be a pointer to a pointer.
    struct GeneralizedColumnList *next;
} GeneralizedColumnList;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;
/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn* gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char* handle;
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    LOAD,
    DELETE,                 // added by DK
    SELECT,                 // added by DK
    FETCH,                  // added by DK
    JOIN,                   // added by DK
    MIN,                    // added by DK
    MAX,                    // added by DK
    SUM,                    // added by DK
    VECTOR_ADD,             // added by DK
    VECTOR_SUBTRACT,        // added by DK
    UPDATE,                 // added by DK
    LOOKUP,                 // added by DK
    NA,
} OperatorType;
/*
 * necessary fields for insertion
 */



typedef struct InsertOperator {
    Column* column;             // Insert requires both the col name and table name as well as a list of values.
    Table* table;
    struct int_list* values;    // values appear in an int_list.
} InsertOperator;
/*
 * necessary fields for insertion
 */
typedef struct LoadOperator {
    char* file_name;
} LoadOperator;

typedef struct DeleteOperator {
    Column* column;             // Delete requires both the col name and table name as well as a list of values.
    Table* table;
    struct int_list* values;    // row vector
} DeleteOperator;

typedef struct SelectOperator {
    Db* db;
    Table* table;
    Column* column;
    int low;
    int high;
} SelectOperator;

typedef struct FetchOperator {
    Db* db;
    Table* table;
    Column* column;
    struct int_list *values;    // these values are row positions.
} FetchOperator;

typedef struct JoinOperator {
    Db* db1;
    Table* table1;
    Column* column1;
    struct int_list* values1;
    Db* db2;
    Table* table2;
    Column* column2;
    struct int_list* values2;
    JoinType jointype;
} JoinOperator;

typedef struct MinOperator {
    Db* db;
    Table* table;
    Column* column;
} MinOperator;

typedef struct MaxOperator {
    Db* db;
    Table* table;
    Column* column;
} MaxOperator;

typedef struct SumOperator {
    Db* db;
    Table* table;
    Column* column;
} SumOperator;

typedef struct VectorAddOperator {
    Db* db;
    Table* table;
    Column* column;
} VectorAddOperator;

typedef struct VectorSubtractOperator {
    Db* db;
    Table* table;
    Column* column;
} VectorSubtractOperator;

typedef struct UpdateOperator {
    Db *db;
    Table* table;
    int old_int;    // the item to be replaced
    int new_int;    // the item to replace it with
} UpdateOperator;

typedef struct PrintOperator {
    struct GeneralizedColumnList gen_list;
} PrintOperator;
/*
 * union type holding the fields of any operator
 */
typedef struct Var {
    char var_name[MAX_SIZE_NAME];
    int_list* var_store;
    struct Var *next;
} Var;

typedef struct DbTblCol {
    char db_name[100];
    char tbl_name[100];
    char col_name[100];
    int col_number;
    int_list *items;
    struct DbTblCol *next;
} DbTblCol;

typedef union OperatorFields {
    InsertOperator insert_operator;
    LoadOperator load_operator;
    DeleteOperator delete_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    JoinOperator join_operator;
    MinOperator min_operator;
    MaxOperator max_operator;
    SumOperator sum_operator;
    VectorAddOperator vector_add_operator;
    VectorSubtractOperator vector_subtract_operator;
    UpdateOperator update_operator;
    PrintOperator print_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

extern Db *current_db;

/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */
Status db_startup();

Db* create_db(const char* db_name, Db *db_head, Var* var_pool);

//Status parse_lookup_db(char* lookup_arguments, Db* db_head);

//Status parse_lookup(char* create_arguments, Db* db_head);

Status create_table(Db* db, char* name, int num_columns, Status *status);

Status create_column(Table *table, char *name, Status *ret_status);

Status name_column(Column *column, const char* col_name, Status *ret_status);

Status insert_row(const char* db_name, struct Db* db_head, const char* table_name, int column_number, struct int_list *list, Status *ret_status);

Status delete_row(const char* db_name, struct Db* db_head, const char* table_name, int column_number, int deletion_row, Status *ret_status);

int_list* select_row(const char* db_name, const char* table_name, const char* column_name, int low, int high, Db* db, const char* var_name, Var* var_pool);

Status fetch_row(const char* db_name, const char* table_name, const char* column_name, GeneralizedColumn* fetch_list);

Status shutdown_server();

void print_col(const char* db_name, GeneralizedColumnList);

char* execute_DbOperator(char query[100], int client_socket, Db* db_head, Var *var_pool);
//was previously char** before - why? does it need to be changed back?

Db* lookup_db(char *name, struct Db* head_db);

Table* lookup_table(char *name, struct Db* db);

Column* lookup_column(char *name, struct Table* table);

void db_operator_free(DbOperator* query);

DbOperator* parse_command(char* query_command, message* send_message, int client, struct ClientContext* context, struct Db* db_head, struct Var* var_pool);

DbOperator* load_db(char *name, struct Db* db_head, Var* var_pool, int client, struct ClientContext* context);

message_status parse_lookup_tbl(char* tokenizer_copy, struct Db* db_head);

void print_db(Db* db_head);

void print_var(Var *var_pool, const char* var_name);

int find_max(char* param1, char* param2, Db* db_head, Var *var_pool);

int find_min(char* param1, char* param2, Db* db_head, Var *var_pool);

void declare_handle(char* name, int_list* result, Var *var_pool);

int_list* interpret_col_or_var (char* param1, Var* var_pool, Db* db_head);

#endif /* CS165_H */

