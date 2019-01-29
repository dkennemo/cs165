#include "cs165_api.h"
#include "utils.h"
#include <string.h>

#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <time.h>
#include <limits.h>

//#define PRIME 100151
//Using lower prime number to test case of multiple hash collisions --
#define PRIME 29
#define DELETE_VALUE -2147483647
#define SIZE_INT_LIST 1018

Db *current_db;

int pow_wow(int base, int exp)
{
    if (exp < 0)
    	return -1;

    int result = 1;
    while (exp)
    {
        if (exp & 1)
            result *= base;
        exp >>= 1;
        base *= base;
    }

    return result;
}

void btree_get(node* rootnode, int item_to_get) {

	//int found_flag = 0, y = 0;
	unsigned int y = 0;
	node* activenode = rootnode;
	while (!activenode->leaf_flag) {
		for (unsigned int x = 0; x < SIZE_INT_LIST / 2; x++)
			if (item_to_get < *activenode->max_array[x])
				activenode = activenode->node_array[x];
		for (y = 0; y < SIZE_INT_LIST; y++)
			if (*activenode->max_array[y] == item_to_get)
				printf("Found data! %li\n", activenode->max_array[y]);
		};
	printf("value from tree is = %li\n", activenode->max_array[y]);
}

void btree_put(node* rootnode, int item_to_add) {

	int found_flag = 0;									// flag set to 1 when value being put in tree has found a home.
	node* activenode = rootnode;
	while (!activenode->leaf_flag) {
		for (unsigned int x = 0; x < SIZE_INT_LIST; x++) {
			if (item_to_add < *activenode->max_array[x])
				activenode = activenode->node_array[x];
		};
	};
	if (activenode->count + 1 > (SIZE_INT_LIST / 2)) {
		// make a new node
		node* new_node = malloc(sizeof(node));
		// copy half of the current node into the new node
		for (unsigned int y = 255; y < (SIZE_INT_LIST / 2); y++) {
			new_node->max_array[y - 255] = activenode->max_array[y];
			new_node->node_array[y - 255] = activenode->node_array[y];
			activenode->max_array[y] = 0;
			activenode->node_array[y] = NULL;
			activenode->count = 255;
			new_node->count = 255;
			};
		}
	else {
		for (int z = activenode->count; z > 0; z--) {
			if (item_to_add <  *activenode->max_array[z]) {
				activenode->max_array[z] = activenode->max_array[z-1];
				activenode->node_array[z] = activenode->node_array[z-1];
			} else {
				*activenode->max_array[z] = item_to_add;
				// also have to add the pointer!
			};
		};
	};
}

node* initialize_btree(node* rootnode, node* reserve_node, fence_pointer* fence, int* occupancy_matrix_ptr) {

static unsigned int NODE_SIZE = SIZE_INT_LIST / 2;   								// fence pointer / node pointers in a given node.
static unsigned int FAN_OUT = SIZE_INT_LIST / 2;    								// Fan out parameter. Each level l has Fan_Out ^ l nodes.
//static unsigned int MAX_LEVELS = 5;   								// Maximum levels of b-tree.

//unsigned long int occupancy_matrix_ptr = malloc(sizeof(unsigned long int) * MAX_LEVELS * 2);

//for (unsigned int x = 0; x < MAX_LEVELS; x++)
//{
//    occupancy_matrix_ptr[x * 2] = pow_wow(FAN_OUT, x); 
//    occupancy_matrix_ptr[x * 2 + 1]  = 0;
//    printf("Occ matrix %i %i\n", occupancy_matrix_ptr[x * 2], occupancy_matrix_ptr[x * 2 + 1]);
//};
  
// create and populate an empty C0 array
//
//array = (int*)malloc(C0_SIZE * 2 * sizeof(int));    // allocate integers to array   *F

// create and populate an empty root node

  struct node newnode;
  rootnode = &newnode;
  *rootnode->max_array = malloc(NODE_SIZE * sizeof(int));
  *rootnode->node_array = malloc(NODE_SIZE * sizeof(int_list*));

// This is a reserve node where values are temporarily placed until last point of merge

  reserve_node = malloc(sizeof(struct node));                
  *reserve_node->max_array = malloc(NODE_SIZE * sizeof(int));
  *reserve_node->node_array = malloc(NODE_SIZE * sizeof(int_list*));

// initialize 50,000 fence pointers (just to pick a number)

  fence = malloc(sizeof(fence_pointer) * 50000);
  for (int x = 0; x < 50000; x++)              // set all max and min references in fence = 0
  {
    fence[x].max_value = 0;
    fence[x].min_value = 0;
    fence[x].counter = 0;
    fence[x].location = NULL;
  };
  fence[0].location = rootnode;         // set up ref to array for new node

return rootnode;
}

void create_column(Table *table, char *name, Status *ret_status) {
	Column* current_col = table->columns;
	if (current_col == NULL) {
		Column* new_column = malloc(sizeof(Column));
		strcpy(new_column->name, name);
		table->columns = new_column;
		return;
	}
	else while(1)
	{
		if (strcmp(name, current_col->name) == 0)
		{
			return;
		}
		else if (current_col->name[0] == '\0')
		{
			strcpy(current_col->name, name);
			return;
		}
		else
		{
			if (current_col->next_col == NULL) {
				Column* new_col = malloc(sizeof(Column));
				current_col->next_col = new_col;
				//new_col->name[0] = '\0';
				new_col->next_col = NULL;
				strcpy(new_col->name, name);
				//current_col = new_col;
				//printf("made new col\n", new_col->name);
				return;  
			}
			current_col = current_col->next_col;
		}
	}
}

void declare_handle(char* name, int_list* result, Var *var_pool) {

	Var* orig_start = var_pool;
	Var* start = var_pool;

	while (orig_start)
		orig_start = orig_start->next;
	orig_start = var_pool;

	// traverse to end of var pool, make sure this var name hasn't been declared before.
	while (start) {
		if (strcmp(name, start->var_name) == 0) {
			start->var_store = result;
			return;
		}
		else
			start = start->next;
	}
	start = var_pool;			// reset to beginning of variable pool

	// create new Var object, populate with result.
	Var* new_var = malloc(sizeof(Var));
	strcpy(new_var->var_name, name);
	new_var->var_store = result;
	new_var->next = NULL;

	// link up pre-existing list to new var object.
	if (start == NULL)
		start = new_var;
	else {
		while (start->next != NULL)
			start = start->next;
		start->next = new_var;
	}

	var_pool = orig_start;

	}


int_list* interpret_col_or_var (char* param1, Var* var_pool, Db* db_head) {
	if (!param1) return NULL;
	char* s = strchr(param1, ')');
	if (s)
		*s = '\0';
	//printf("interpreting %s\n", param1);
	char* tbl_name = malloc(100);
	char* db_name = malloc(100);
	char* col_name = malloc(100);
	strcpy(tbl_name, param1);
	tbl_name = divide_period(tbl_name);
	if (tbl_name == NULL)
	{
		// is var type
		Var* current = var_pool;
		while (current != NULL) {

			if (strcmp(param1, current->var_name) == 0)
				return(current->var_store);
			if (current->next)
				current = current->next;
			else {
				//printf("couldn't find var in list\n");
				return NULL; 
			};
		};
	} else {

		strcpy(db_name, param1);
		s = strchr(db_name, '.');
		*s = '\0';

		strcpy(col_name, tbl_name);
		col_name = divide_period(col_name);
		s = strchr(col_name, ')');
		if (s)
			*s = '\0';

		s = strchr(tbl_name, '.');
		*s = '\0';
		//printf("tbl name = %s\n", tbl_name);


		Db* db_search = lookup_db(db_name, db_head);
		Table* tbl_search = lookup_table(tbl_name, db_search);
		Column* col_search = lookup_column(col_name, tbl_search);
		if (col_search == NULL) {
			//printf("Error in tracking down %s.%s.%s\n", db_name, tbl_name, col_name);
			return NULL;
		}
		else {
			//printf("Found column %s\n", col_name);
			return col_search->data;
		};
	};
return 0;
}

int find_sum(char* param1, Db* db_head, Var* var_pool) {

	int_list* param1_list;
	int sum = 0;

	param1_list = interpret_col_or_var(param1, var_pool, db_head);
	if (param1_list == NULL) {
		//printf("list is empty\n");
		return 0;
	}
	else {
		//printf("Summing %s\n", param1);
		while (param1_list) {
			for (unsigned int x = 0; x < param1_list->count; x++)
				sum += param1_list->item[x];
			param1_list = param1_list->next; 
		};
	};
	log_info("%i\n", sum);
	return sum;
}

int find_avg(char* param1, Db* db_head, Var* var_pool) {

	int_list* param1_list;
	double sum = 0;
	double count = 0;

	param1_list = interpret_col_or_var(param1, var_pool, db_head);
	if (param1_list == NULL) {
		printf("list is empty\n");
		return 0;
	}
	else {
		//printf("Averaging %s\n", param1);
		while (param1_list) {
			for (unsigned int x = 0; x < param1_list->count; x++) {
				sum += param1_list->item[x];
				printf("+= ", param1_list->item[x]);
				count++;
			}
			param1_list = param1_list->next; 
		};
	};
	double avg = sum / count;

	log_info("%lf.2\n", avg);
	return (avg);
}

int find_max(char* param1, char* param2, Db* db_head, Var* var_pool) {

int max = -2147483646;
int_list* param1_list;
int_list* param2_list;

param1_list = interpret_col_or_var(param1, var_pool, db_head);
param2_list = interpret_col_or_var(param2, var_pool, db_head);

if (param1 == NULL && param2 == NULL) {
	//printf("Neither input list has been found\n");
	return 0;
}
else if (param1 != NULL && param2 == NULL) {
	//printf("Searching for simple max of param1 list\n");
	int_list* start = param1_list;
	while (start) {
		for (unsigned int x = 0; x < start->count; x++) {
			if (start->item[x] > max)
				max = start->item[x];
		}
		start = start->next;
	};
	log_info("%i\n", max);
	return max;
}
else if (!param1_list && param2_list) {
	printf("searching for the position in param2 list that is the max\n");
	int pos_of_max = -1;
	int pos_in_file = -1;
	int_list* start = param2_list;
	while (start) {
		for (unsigned int x = 0; x < start->count; x++) {
			pos_in_file++;	
			if (start->item[x] > max) {
				max = start->item[x];
				pos_of_max = pos_in_file;
			};
			start = start->next;
		};
	};
	printf("position of max value = %i\n", pos_of_max);
	return pos_of_max;
}
// ??? COME BACK TO THIS ONE ???
else {
	printf("searching for max of param1 list from positions in param 2 list\n");
	int_list* start1 = param1_list;
	int_list* start2 = param2_list;
	do {
		for (unsigned int x = 0; x < param2_list->count; x++)
		{
			while(start2) {
				for (unsigned int y = 0; y < start2->count; x++)
					if (start1->item[x] > max) max = start1->item[x];
				start2 = start2->next;
			};
		};
		if (start2->next != NULL)
			start2 = start2->next;
	}
	while (param2_list);
	return max;
};
}

int find_min(char* param1, char* param2, Db* db_head, Var* var_pool) {

int min =  2147483647;

int_list* param1_list = interpret_col_or_var(param1, var_pool, db_head);
int_list* param2_list = interpret_col_or_var(param2, var_pool, db_head);

if (!param1_list && !param2_list) {
	//printf("Neither input list has been found\n");
	return 0;
}
else if (param1_list && !param2_list) {
	//printf("Searching for simple min of param1 list\n");
	int_list* start = param1_list;
	while (start) {
		for (int x = 0; x < start->count; x++) {
			if (start->item[x] < min && start->item[x] != DELETE_VALUE)
				min = start->item[x];
		};
		start = start->next;
	};
	return min;
// ??? COME BACK TO THIS ONE
} else if (!param1_list && param2_list) {
	printf("searching for the position in param2 list that is the min\n");
	int pos_of_min = -1;
	int pos_in_file = -1;
	int_list* start = param2_list;
	while (start) {
		for (unsigned int x = 0; x < start->count; x++) {
			pos_of_min++;
			if (start->item[x] < min && start->item[x] != DELETE_VALUE) {
				min = start->item;
				pos_of_min = pos_in_file;
			}; 
		};
		start = start->next;
	};
	return pos_of_min;
}
// ??? COME BACK TO THIS ONE
else {
	printf("searching for min of param1 list from positions in param 2 list\n");
	int_list* start1 = param1_list;
	int_list* start2 = param2_list;
	int pos_in_param1_list = 0;
	int pos_in_param2_list = 0;
	int min;
//	while(start2) {
//		for (unsigned int x = pos_in_param1_list; x < start2->item; x++)
//			start1 = start1->next;
//		if (start1->item > min && start1->item != DELETE_VALUE)
//			min = start1->item[x];
//		start2 = start2->next;
//	};
	return min;
}
}

int_list* find_add(char* param1, char* param2, Db* db_head, Var* var_pool) {

if (param1 == NULL || param2 == NULL) {
	printf("Two vectors are required. Exiting.\n");
	return NULL;
};

int_list* param1_list = interpret_col_or_var(param1, var_pool, db_head);
int_list* param2_list = interpret_col_or_var(param2, var_pool, db_head);

int_list* start1 = param1_list;
int_list* start2 = param2_list;
int_list* vector_add = malloc(sizeof(int_list));
int_list* vector_backup = vector_add;

while (start1 && start2) {
	for (unsigned int x = 0; x < start1->count; x++) {
		vector_add->item[x] = start1->item[x] + start2->item[x];
	};
	vector_add->count = start1->count;
	start1 = start1->next;
	start2 = start2->next;
	if (start1 && start2) {
		int_list* vector_new = malloc(sizeof(int_list));
		vector_add->next = vector_new;
		vector_add = vector_new;
	};
};
return vector_backup;
}

int_list* find_sub(char* param1, char* param2, Db* db_head, Var* var_pool) {

if (param1 == NULL || param2 == NULL) {
	printf("Two vectors are required. Exiting.\n");
	return NULL;
};

int_list* param1_list = interpret_col_or_var(param1, var_pool, db_head);
int_list* param2_list = interpret_col_or_var(param2, var_pool, db_head);

int_list* start1 = param1_list;
int_list* start2 = param2_list;
int_list* vector_sub = malloc(sizeof(int_list));
int_list* vector_backup = vector_sub;

while (start1 && start2) {
	for (unsigned int x = 0; x < start1->count; x++) {
		vector_sub->item[x] = start1->item[x] - start2->item[x];
	};
	vector_sub->count = start1->count;
	start1 = start1->next;
	start2 = start2->next;
	if (start1 && start2) {
		int_list* vector_new = malloc(sizeof(int_list));
		vector_sub->next = vector_new;
		vector_sub = vector_new;
	};
};
return vector_backup;
}

void print_var(Var *var_pool, const char* create_arguments, int client_socket) {

	//printf("var pool = %p", var_pool);
	//printf("args: %s\n", create_arguments);
    message_status mes_status;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));

    char *token = '\0';
    strcpy(tokenizer_copy, create_arguments);
    printf("%s\n", tokenizer_copy);
 	token = trim_parenthesis(tokenizer_copy);
/*    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        token = next_token(&tokenizer_copy, &mes_status);
    	int last_char = strlen(token) - 1;
   		if (token[last_char] != ')') {
	       	printf("syntax error\n");
	       	return;
   		}
	    else	
	       	token[last_char] = '\0'; */
 	    //printf("variable name = %s\n", token);
    	//}// 

		Var* start = var_pool;

		//printf("var pool value = %p, start value = %p\n", var_pool, start);

		//printf("looking for var name %s\n", token);
		if (start == NULL)
			printf("var pool is empty - nothing to print\n");
		else do
		{
			message to_send;
			char send_buffer[1024];
			int length;
			//printf("comparing %s %li %s %li\n", start->var_name, strlen(start->var_name), token, strlen(token));
			if (strcmp(start->var_name, token) == 0) {
				//printf("setting up int_list to point to the var_store\n");
				int_list* results = malloc(sizeof(int_list*));
				results = start->var_store;
				int_list* results_to_print = malloc(sizeof(int_list*));
				results_to_print = results;
				if (results != NULL) {
					while (results_to_print != NULL)
					{
						//printf("number of results in this node = %i\n", results_to_print->count);
						char* result = malloc(100);
						for (int x = 0; x < results_to_print->count; x++) {
							printf("%i\n", results_to_print->item[x]);
						}
						results_to_print = results_to_print->next;
					};
					printf("\n");
//					return NULL;	
				}
			};
			start = start->next;
		} 
		while (start != NULL);
}

int_list* fetch_row(const char* db_name, const char* table_name, const char* column_name, const char* var_name, Db* db_head, char* handle, Var* var_pool) {

Var* start = var_pool;
// initialize a list of items to return and the working pointer for the same
// this is the actual OUTPUT of this function
int_list* items_to_return = malloc(sizeof(int_list));
int_list* items_to_return_working = items_to_return;
// initialize the pointers to the source data - the indexes (produced from an earlier select)
// will get the items from here
int_list* items_to_fetch_from = malloc(sizeof(int_list));
int_list* items_to_fetch_from_working = items_to_fetch_from;
// Initialize the pointers to the list of indexes. 
int_list* items_to_fetch = malloc(sizeof(int_list));
int_list* items_to_fetch_working = items_to_fetch;

int index_node = 0;

// find the db, table and col
// the int_list pointed to by the col is actual source data
Db* target_db = lookup_db(db_name, db_head);
int found_var_name = 0;
Table* target_table = lookup_table(table_name, target_db);
//printf("looking for column %s in table %s\n", column_name, target_table->name);
Column* target_column = lookup_column(column_name, target_table);
if (target_column == NULL)
{
	printf("Couldn't find the specified column in the db/table combo. Can't execute fetch.\n");
	free(items_to_return);
	free(items_to_fetch_from);
	free(items_to_fetch);
	return NULL;
};
items_to_fetch_from = target_column->data;


// now find the start of the var_name list:
// the int_list pointed to here is the list of the positions to draw items from
// items_to_fetch_from
//printf("var pool value = %p, start value = %p\n", var_pool, start);
//printf("looking for var name %s\n", var_name);
if (start == NULL)
	printf("var pool is empty - nothing to print\n");
else do
{
	if (strcmp(start->var_name, var_name) == 0) {
		//printf("setting up int_list to point to the var_store\n");
		items_to_fetch = start->var_store;
		items_to_fetch_working = items_to_fetch;
	};
	start = start->next;
} 
while (start != NULL);

int find_item = 0;
while (items_to_fetch_working != NULL) {
	for (unsigned int x = 0; x < items_to_fetch_working->count; x++) {
		find_item = items_to_fetch_working->item[x];
		//printf("The current page ends at item # %i\n", (items_to_fetch_from->count + index_node));
		while ((items_to_fetch_from->count + index_node) < find_item) {
			index_node += items_to_fetch_from->count;
			items_to_fetch_from = items_to_fetch_from->next;
		};
		items_to_return_working->item[items_to_return_working->count++] = items_to_fetch_from->item[find_item - index_node];
		//items_to_fetch_return_working->item[x] = 0; // replace with gotten item
	};
	items_to_fetch_working = items_to_fetch_working->next;
	int_list* new_items = malloc(sizeof(int_list));
	new_items->count = 0;
	items_to_return_working->next = new_items;
	}
return items_to_return;
}

int_list* select_row_batch(DbTblCol* batch_start, Db* head_db, Var* var_pool) {

// create a working copy pointer to batch_start
DbTblCol* batch_working = batch_start;
Column* target_column_backup;
Column* target_column;

printf("In select_row_batch fxn\n");
printf("batch_working = %i", batch_working);
while (batch_working != NULL) {
	// find the db, table and col for the batch select
	Db* target_db = batch_working->db_name;
	Table* target_table = lookup_table(batch_working->tbl_name, target_db);

	// look for column
	Column* target_column = lookup_column(batch_working->col_name, target_table);
	target_column_backup = target_column;
	if (target_column == NULL)
		printf("Couldn't find the specified column in the db/table combo. Can't execute batch select.\n");
	int_list* selected_items = malloc(sizeof(int_list));			// declare a pointer to an int_list
	//int_list* selected_items_return = selected_items;				// create a "select list" to track what is returned

	printf("DECLARING handle %s\n", batch_working->handle);
	declare_handle(batch_working->handle, selected_items, var_pool);
	batch_working->items = selected_items;
	batch_working = batch_working->next;
};

// Now reset and go though each column page at a time, assigning qualifying items to each respective
// var's select.

batch_working = batch_start;

// Unlike regular select, in batch select the var names are assigned at the time where all
// these selects are actually executed (which is in this function). The best way to do that is
// once results are actually being SOUGHT for a given command in the series, create and point to
// the newly-formed int_list.

	int tuple = 0;

	while(target_column->data != NULL)
	{
		printf("looping from 0 to %i\n", target_column->data->count);
		for (unsigned int x = 0; x < target_column->data->count; x++) {
			while(batch_working != NULL) {
			if (target_column->data->item[x] >= batch_working->low && target_column->data->item[x] < batch_working->high  &&
				target_column->data->item[x] != DELETE_VALUE) // if contents of tgt col reserve->data is in the range...
			{
				log_info("%i\n", target_column->data->item[x]);
				if (batch_working->items->count == 1018) {										// if full...
					int_list* new_selected_items = malloc(sizeof(int_list));
					new_selected_items->count = 0;
					batch_working->items->next = new_selected_items;
					batch_working->items = batch_working->items->next; // need a backup to resest so you don't lose orig node!
				};
				batch_working->items->item[batch_working->items->count] = tuple;
				printf("value %i = %i \n",batch_working->items->count, tuple);
				batch_working->items->count++;
			};
			tuple++;
			};
		};
		printf("ended loop\n");
		target_column->data = target_column->data->next;
	};
	target_column = target_column_backup;
//	target_column->data = data_backup;

	printf("\n");
	batch_working = batch_working->next;
	//return selected_items_return;
	return target_column;
};

int_list* select_row(const char* db_name, const char* table_name, const char* column_name, int low, int high, Db* head_db, const char* var_name, Var* var_pool) {

Status status;

// find the db, table and col
Db* target_db = lookup_db(db_name, head_db);
int found_var_name = 0;
Table* target_table = lookup_table(table_name, target_db);

//printf("looking for column %s in table %s\n", column_name, table_name);
Column* target_column = lookup_column(column_name, target_table);
if (target_column == NULL)
{
	//printf("Couldn't find the specified column in the db/table combo. Can't execute select.\n");
	return NULL;
};

//printf("Assigning to %s\n", var_name);

Column* target_column_backup = target_column;
int_list* data_backup = target_column->data;
int_list* selected_items = malloc(sizeof(int_list));			// declare a pointer to an int_list
int_list* selected_items_return = selected_items;				// create a "select list" to track what is returned
int tuple = 0;

while(target_column->data != NULL)
{
	//printf("looping from 0 to %i\n", target_column->data->count);
	for (unsigned int x = 0; x < target_column->data->count; x++) {
		if (target_column->data->item[x] >= low && target_column->data->item[x] < high
			&& target_column->data->item[x] != DELETE_VALUE) // if contents of tgt col reserve->data is in the range...
		{
			//printf("%i ", target_column->data->item[x]);
			if (selected_items->count == 1018) {										// if full...
				int_list* new_selected_items = malloc(sizeof(int_list));
				new_selected_items->count = 0;
				selected_items = new_selected_items;
			};
			selected_items->item[selected_items->count] = tuple;
			//printf("value %i = %i \n", selected_items->count, tuple);
			selected_items->count++;
		};
		tuple++;
	}
	//printf("ended loop\n");
	target_column->data = target_column->data->next;
};
target_column = target_column_backup;
target_column->data = data_backup;

printf("\n");
return selected_items_return;
}


void create_table(Db* db_current, char* name, int num_columns, Status *ret_status) {
	// If no table exists, create one
	if (db_current->tables == NULL) {				
		//printf("table list is empty - creating a new one...\n");
		//printf("table name %s\n", name);
		struct Table *new_table = (struct Table*) malloc(sizeof(struct Table));
		strcpy(new_table->name, name);	// copy the name
		new_table->col_count = num_columns;
		new_table->next_tbl = NULL;
		new_table->table_length = 0;
		db_current->tables = new_table;
		Column* current_col = malloc(sizeof(Column));
		Column* start_col = current_col;
		//printf("Addr of start col = %i\n", start_col);
		new_table->columns = current_col;
		int_list* current_col_data = malloc(sizeof(int_list));
		current_col_data = NULL; // keep?
		new_table->columns->data = current_col_data;
		for (int x = 1; x < num_columns; x++) { 
			// create a new column node
			Column* new_column = malloc(sizeof(Column));
			// create and initialize a new col
			int_list* new_col_data = malloc(sizeof(int_list));
			new_col_data->count = 0;
			// hook column to new int list
			new_column->data = new_col_data;

			new_column->name[0] = '\0';

			current_col->next_col = new_column;
			//printf("curr-col-next points to --> %i\n", current_col->next_col);
			current_col = new_column;
			//printf("added col %i ...", x);
			//printf("addr of latest column added is %i\n", current_col);
		}
		new_table->columns = start_col;
		//printf("addr of START column is %i\n", new_table->columns);
	}
	else {
		//printf("tbl list is not empty - having to traverse...\n");
		struct Table *temp_tbl = lookup_table(name, db_current);		
		if (temp_tbl == NULL) {									// In this case, the wasn't found. So create it...
			//printf("couldn't find decalred name in the existing list, so creating new with %i columns...\n", num_columns);
			struct Table *new_table = (struct Table*) malloc(sizeof(struct Table));		// Go ahead and make it
			temp_tbl = db_current->tables;
			while (temp_tbl->next_tbl != NULL)
				temp_tbl = temp_tbl->next_tbl;
			temp_tbl->next_tbl = new_table;
			//while (db_end->next_db != NULL)
			//	db_end = db_end->next_db;
			//db_end->next_db = new_db;
			strcpy(new_table->name, name);	// copy the name
			new_table->col_count = num_columns;		// initialize other variables to indicate a totally empty db with
			new_table->next_tbl = NULL;
			new_table->table_length = 0;
			//printf("table name is %s\n", new_table->name);
			Column* current_col = malloc(sizeof(Column));
			new_table->columns = current_col;
			for (int x = 1; x < num_columns; x++) {
				Column* new_column = malloc(sizeof(Column));
				//current_col->name = malloc(sizeof(char) * 64);
				current_col->data = NULL;
				current_col->name[0] = '\0';
				current_col->next_col = new_column;
				current_col = new_column;
			}
		}
	}
//	ret_status->code = OK;
return;
//	return *ret_status;
}


/* 
 * Similarly, this method is meant to create a database.
 */
Db* create_db(const char* db_name, Db* db_head, Var* var_pool) {
	(void) (db_name);
	struct Status ret_status;
	fflush(stdout);
	if (db_head->empty_flag == 1) {				// what if there are NO dbs (i.e., list of present dbs is empty?)
		fflush(stdout);
		strcpy(db_head->name, db_name);	// copy the name
		db_head->tables = NULL;			// initialize other variables to indicate a totally empty db with
		db_head->tables_size = 0;		// no tables and pointing to nothing else.
		db_head->tables_capacity = 0;
		db_head->next_db = NULL;
		db_head->empty_flag = 0;
		return db_head;
	}
	else {
		struct Db *temp_db = lookup_db(db_name, db_head);		
		if (temp_db->next_db == NULL) {									// In this case, the wasn't found. So create it...
			struct Db *new_db = (struct Db*) malloc(sizeof(struct Db));		// Go ahead and make it
			temp_db->next_db = new_db;
			//while (db_end->next_db != NULL)
			//	db_end = db_end->next_db;
			//db_end->next_db = new_db;
			strcpy(new_db->name, db_name);	// copy the name
			new_db->tables = NULL;			// initialize other variables to indicate a totally empty db with
			new_db->tables_size = 0;		// no tables and pointing to nothing else.
			new_db->tables_capacity = 0;
			new_db->next_db = NULL;
			new_db->empty_flag = 0;
			//printf("db name is %s\n", temp_db->name);
			return new_db;
		};
	};
	return db_head;		// just posted here to defease the warning.
}
// Checked over - this function looks ok
Status name_column(struct Column *column, const char* col_name, Status *ret_status) {
	Status localStatus = *ret_status;
	strcpy(col_name, column->name);
	localStatus.code = OK;
	return localStatus;
}

int_list* delete_row(const char* db_name, struct Db* db_head, const char* table_name, 
	const char* var_name, Var* var_pool) {

		// Find the database, table and first column given the db_name and table_name inputs.

		Column* col;
		Db* db_search = lookup_db(db_name, db_head);
		Table* tbl_search = lookup_table(table_name, db_search);
		if (tbl_search)
			col = tbl_search->columns;

		// return null if column can't be found.

		if (col == NULL) {
			//printf("Error in tracking down %s.%s\n", db_name, table_name);
			return NULL;
		}
		else {
			// find the var and traverse list of items to delete
			Var* var_working = var_pool;
			int_list* delete_list;
			while (var_working)
			{
				// is the var name in the variable pool list?
				// if yes, this is dubbed the "delete_list"
				if (strcmp(var_working->var_name, var_name) == 0) {
					delete_list = var_working->var_store;
					//printf("created delete column from variable $s\n", var_working->var_name);
					break;
				 }
				// if not, traverse...
				else
					var_working = var_working->next;
			};
			// was the var name ultimately found? if not, return null.
			if (var_working == NULL)
				return NULL;

			// find the int_list associated with the variable var_working
			int_list* var_store_working = var_working->var_store;

			// is the variable store working variable occupied?
			while (var_store_working != NULL) {
			// if so, loop through the items in that int list...
				for (unsigned int x = 0; x < var_store_working->count; x++) {
			// each row is deemed a "row to delete"
					int row_to_delete = var_store_working->item[x];
					Column* temp_col = col;
					// loop through the columns...
					//printf("Eliminating values\n");
					while (temp_col)
					{
						//printf("...from column %s\n", temp_col->name);
					// in each iteration, find the data row and set to the presumptive deleted value
						int_list* working_int_list = temp_col->data;
						//printf("item %i ", x);
						temp_col->data->item[var_working->var_store->item[x]] = DELETE_VALUE;
					// tee up the next column to repeat this for them all.
						temp_col = temp_col->next_col;
					}; 
				};
			var_store_working = var_store_working->next;
			};
		};
return col->data;
}

int_list* join(struct Db* db_head, Var* var_pool, int_list* var_1, int_list* var_2, int_list* positions_1, int_list* values_1, int_list* positions_2,
	int_list* values_2, int join_type, char* r1_name, char* r2_name) {

// This function does the following: Places into var_1 and var_2 positions and values of the joining of positions and values
// from lists 1 and 2.
//
// Nested loop version:
//
// Loop through all values in values_1
//		Loop through all values in values_2
//			Is value_1 item = value_2 item?
//				If so, add position_1 to array 1 AND add position 2 to array 2
// Iterate through array 1 and array 2 to eliminate x, y followed by y, x
// Create new table, called John_[name of Var 1]_[name of Var 2]
// Create two columns in the table, R1 and R2
// Link columns to array 1 and array 2, respectively.
//

// The outputs are var_1 and var_2 -- add their handles to the var pool.
int_list *var_1_working = malloc(sizeof(int_list));
var_1_working = var_1;	

int_list *var_2_working = malloc(sizeof(int_list));
var_1_working = var_2;

int_list *positions_1_working = positions_1;
int_list *positions_2_working = positions_2;
int_list *values_1_working = values_1;
int_list *values_2_working = values_2;

if (join_type == 0) {					// 0 means used nested loop version of join algo

//printf("performing nested loop\n");
int index_x = 0;
int index_y = 0;
while (values_1_working != NULL) {
	for (unsigned int x = 0; x < values_1_working->count; x++)
		{
		values_2_working = values_2;
		while (values_2_working != NULL) {
			index_y = 0;
			for (unsigned int y = 0; y < values_2_working->count; y++)
				{
				if (values_1_working->item[x] == values_2_working->item[y]) {
					//printf("item #s: x = %i & y = %i. Comparison: %i and %i\n",(index_x + x), (index_y + y),
					//	values_1_working->item[x], values_2_working->item[y]);

					var_1_working->item[var_1_working->count++] = positions_1_working->item[x];
					// revisit how this will work when positions_1_working goes beyond the first node
					var_2_working->item[var_2_working->count++] = positions_2_working->item[y];
					// revisite how this will work when positions_2_working goes beyond the first node
					};
				};
			index_y += values_2->count;
			values_2_working = values_2_working->next;
			};
		};
	index_x += values_1->count;
	values_1_working = values_1_working->next;
	};
var_1 = var_1_working;
var_2 = var_2_working;	
declare_handle(r1_name, var_1, var_pool);
declare_handle(r2_name, var_2_working, var_pool);
}
else if (join_type == 1) {

//printf("performing hash join\n");
hash* hashtable = create_hash_table(values_1_working);
int index_x = 0;
int index_y = 0;
while (values_2_working != NULL) {
	for (unsigned int x = 0; x < positions_2_working->count; x++)
		{
			//printf("testing #%i, item = %i ... ", x, values_2_working->item[x]);
			int hash_result = hash_test(hashtable, values_2_working->item[x]);
			if (hash_result != -1) {
				//printf("matched %i ", values_2_working->item[x]);
				var_1_working->item[var_1_working->count++] = positions_1_working->item[x];
				// revisit how this will work when positions_1_working goes beyond the first node
				var_2_working->item[var_2_working->count++] = hash_result;
				// revisite how this will work when positions_2_working goes beyond the first node
			};
		};
values_2_working = values_2_working->next;
};
var_1 = var_1_working;
var_2 = var_2_working;	
declare_handle(r1_name, var_1, var_pool);
declare_handle(r2_name, var_2, var_pool);

};
// free hash table here!
return 0;
}

hash* create_hash_table (int_list *values) {

	// initialize hash table with null values
	int number_of_hash_buckets = PRIME;
	hash* hash_table = malloc(sizeof(hash) * number_of_hash_buckets);
	for (int z = 0; z < number_of_hash_buckets; z++)
		hash_table[z].value = 0;

	int int_hash;

	//printf("hashing %i values\n", values->count);
	for (unsigned int x = 0; x < values->count; x++)
	{
		//printf("looking at item# %i", x);
		int_hash = values->item[x] % number_of_hash_buckets;
		//printf("hashes to %i ", int_hash);
		hash* hash_pointer = hash_table + sizeof(hash) * int_hash;
		if (hash_pointer->value == 0) {
			//printf("First item that hashes to %i ", int_hash);
			hash_pointer->value = values->item[x];
			//printf("no colission @ hash pointer->value %i hashes to %i\n", hash_pointer->value, int_hash);
			hash_pointer->pos = x;
			hash* hash_pointer_new = malloc(sizeof(hash));
			hash_pointer_new->value = 0;
			hash_pointer_new->pos = x;
			hash_pointer_new->next = NULL;
			hash_pointer->next = hash_pointer_new;
		}
		else {
			//printf("collision @ hash_pointer->value %i hashes to %i\n", hash_pointer->value, int_hash);
			hash* hash_pointer_working = hash_pointer;
			hash* hash_pointer_temp = NULL;
			while (hash_pointer_working != NULL) {
				hash_pointer_temp = hash_pointer_working;
				hash_pointer_working = hash_pointer_working->next;
			};
			hash_pointer_temp->next = malloc(sizeof(hash));
			hash_pointer_temp = hash_pointer_temp->next;
			hash_pointer_temp->value = values->item[x];
			hash_pointer_temp->pos = x;
			hash_pointer_temp->next = NULL;
		};
	};
return hash_table;
}

int hash_test (hash* hash_table, int value) {

	int number_of_hash_buckets = PRIME;
	int int_hash = value % number_of_hash_buckets;
	hash* hash_pointer = hash_table + sizeof(hash) * int_hash;
	if (hash_pointer->value == 0) {
		//printf("checked ht for %i but was empty -- \n", value);
		return -1;
	}
	else {
		//printf("hash not empty! \n");
		while (hash_pointer != NULL) {
			if (hash_pointer && hash_pointer->value == int_hash) {
				printf("%i:%i\n", hash_pointer->value, hash_pointer->pos);
				return hash_pointer->pos;
				};
			//else if (hash_pointer)
			//	printf("found %i hash pointer value, not the same as %i int_hash\n", hash_pointer->value, int_hash);
			hash_pointer = hash_pointer->next;

		};
		return -1;
	};
}

void quicksort(int A[], int B[], int lo, int hi)	//  A[] is what is sorted, B[] is the set of indexes
{
  int p = 0;
  int* lo_ptr = &lo;
  int* hi_ptr = &hi;
  if (lo < hi)
  {
    p = partition(A, B, lo_ptr, hi_ptr);
    quicksort(A, B, lo, p - 1);
    quicksort(A, B, p + 1, hi);
  };
}

int partition(int A[], int B[], int* lo, int* hi)
{
  int j = 0;
  int k = 0;
  int x = *lo;
  int y = *hi + 1;
  int pivot = A[*lo];


  while (1)
  {
    do ++x; while (A[x] < pivot && x <= *hi);
    do --y; while (A[y] > pivot);
    if (x >= y) break;
    j = A[x];
    k = B[x];
    A[x] = A[y];
    B[x] = B[y];
    A[y] = j;
    B[y] = k;
  };
  j = A[*lo];
  k = B[*lo];
  A[*lo] = A[y];
  B[*lo] = B[y];
  A[y] = j;
  B[y] = k;
  return (y);
}

