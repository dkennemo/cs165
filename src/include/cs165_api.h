

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

// Each node contains a pointer to an array. Originally I was considering
// variable scale in the array associated with a node, but ultimately settled on
// one size for each (with each level simply having as many nodes as required to
// suit.

typedef struct node {
  int *max_array[509];
  struct node *node_array[509];
  bool leaf_flag;
  int count;
} node;

typedef struct fence_pointer
{
  int min_value;
  int max_value;
  unsigned int counter;
  struct node *location;     // Need to decide whether to keep this.
} fence_pointer;


// DK: The purpose of the following structure is to enable a dynamic length list of inputs for insertion operator.

typedef struct int_list {
    int item[1024 - 6];         // (1024 - 6) * 4 = size of the array
    int low, high;
    unsigned int count;
    struct int_list *next;
} int_list;


typedef struct Stats {
    float mean;
    float sdev;
    float low1, high1, low2, high2, low3, high3, low4, high4;
    float min, max;
    int quan1, quan2, quan3, quan4;
} Stats;

typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    struct int_list* data;                 // DK: substituted int_list for int to enable pointers.
    bool index_present;                     // 1 = Yes, indexed; 0 = No, not indexed
    int *index;                             // depending on type of index contains either logical or physical order of data
    bool btree;                             // 1 = index is a b-tree; 0 = index is a sorted column
    bool clustered;                         // 1 = clustered, 0 = unclustered
    Stats stats;
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
    unsigned int col_count;
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
/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */

typedef enum DataType{
    FLOAT,
    INT,
    LONG,
} DataType;

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
typedef struct Var {
    char var_name[MAX_SIZE_NAME];
    int_list* var_store;
    struct Var *next;
} Var;

typedef struct Batch_list {
    char query[100];
    struct Batch_list* next;
} Batch_list;

typedef struct DbTblCol {
    char db_name[100];
    char tbl_name[100];
    char col_name[100];
    int col_number;
    char handle[100];     // used for batch select only
    int low;              // used for batch select only
    int high;             // used for batch select only
    int_list *items;
    struct DbTblCol *next;
} DbTblCol;

typedef struct hash {
    int value;
    int pos;
    struct hash* next;
} hash;

/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    //OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

extern Db *current_db;

/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */
Status db_startup();

void btree_put(struct node*, int);

Db* create_db(const char* db_name, Db *db_head, Var* var_pool);

//Status parse_lookup_db(char* lookup_arguments, Db* db_head);

//Status parse_lookup(char* create_arguments, Db* db_head);

void create_table(Db* db, char* name, int num_columns, Status *status);

void create_column(Table *table, char *name, Status *ret_status);

Status name_column(Column *column, const char* col_name, Status *ret_status);

Status insert_row(const char* db_name, struct Db* db_head, const char* table_name, int column_number, struct int_list *list, Status *ret_status);

int_list* delete_row(const char* db_name, struct Db* db_head, const char* table_name, const char* var_name, Var* var_pool);

int_list* select_row(const char* db_name, const char* table_name, const char* column_name, int low, int high, Db* db, const char* var_name, Var* var_pool);

int_list* select_row_batch(DbTblCol* batch_start, Db* head_db, Var* var_pool);

int_list* fetch_row(const char* db_name, const char* table_name, const char* column_name, const char* var_name, Db* db_head, char* handle, Var* var_pool);

Status shutdown_server();

//void print_col(const char* db_name, GeneralizedColumnList);

char* execute_DbOperator(char query[100], int client_socket, Db* db_head, Var *var_pool);
//was previously char** before - why? does it need to be changed back?

Db* lookup_db(char *name, struct Db* head_db);

Table* lookup_table(char *name, struct Db* db);

Column* lookup_column(char *name, struct Table* table);

void db_operator_free(DbOperator* query);

DbOperator* parse_command(char* query_command, message* send_message, int client, struct ClientContext* context, struct Db* db_head, struct Var* var_pool, int* batch_mode, DbTblCol* batch);

DbOperator* load_db(char *name, struct Db* db_head, Var* var_pool, int client, struct ClientContext* context, int* batch_mode, DbTblCol* batch);

message_status parse_lookup_tbl(char* tokenizer_copy, struct Db* db_head);

void print_db(Db* db_head);

void print_var(Var *var_pool, const char* var_name, int client_socket);

int find_max(char* param1, char* param2, Db* db_head, Var *var_pool);

int find_min(char* param1, char* param2, Db* db_head, Var *var_pool);

int find_sum(char* param1, Db* db_head, Var* var_pool);

int find_avg(char* param1, Db* db_head, Var* var_pool);

// find_add and find_sub return a *vector* of values so have ot take the form int_list rather than one int
// like some of the other aggregates.

int_list* find_add(char* param1, char* param2, Db* db_head, Var *var_pool);

int_list* find_sub(char* param1, char* param2, Db* db_head, Var *var_pool);

void declare_handle(char* name, int_list* result, Var *var_pool);

int_list* interpret_col_or_var (char* param1, Var* var_pool, Db* db_head);

void quicksort(int A[], int B[], int lo, int hi);

int partition(int A[], int B[], int* lo, int* hi);

hash* create_hash_table (int_list *values); 

int hash_test (hash* hash_table, int value);

//int* initialize_btree(struct node* rootnode, struct node* reserve_node, struct fence_pointer* fence, int* occupancy_matrix_ptr);

/* stolen from cs265

int flush (struct fence_pointer*, unsigned int [][2], unsigned int, struct node*, int, int, FILE*[], struct bloom_array*, FILE*);

void quicksort (int*, int, int);

int partition(int*, int*, int*);

unsigned int bloom_check(int, struct bloom_array* bloom_filter);

unsigned int bloom_set(int, struct bloom_array* bloom_filter);

unsigned int bloom_clear(struct bloom_array* bloom_filter, int);

int get_key(int, int*, FILE*, unsigned int[][2], struct bloom_array*, struct fence_pointer[], int);

*/

#endif /* CS165_H */

