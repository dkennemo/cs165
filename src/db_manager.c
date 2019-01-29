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
	printf("CREATE COLUMN ROUTINE. \nsearching for the first empty column in %s (and also checking avail of name %s)\n", table->name, name);
	Column* current_col = table->columns;
	if (current_col == NULL) {
		printf("hmm.. didn't initialize with any columns!\n");
		//ret_status->code = OK;
		return;
	}
	else while(1)
	{
		if (strcmp(name, current_col->name) == 0)
		{
			printf("you already have a column named %s! try again.\n", name);
			return;
		}
		else if (current_col->name[0] == '\0')
		{
			printf("adding column name %s\n", name);
			strcpy(current_col->name, name);
			return;
		}
		else
		{
			if (current_col->next_col == NULL) {
				strcpy(current_col->name, name);
				Column* new_col = malloc(sizeof(Column));
				current_col->next_col = new_col;
				new_col->name[0] = '\0';
				new_col->next_col = NULL;
				//current_col = new_col;
				printf("made new col %s, dude\n", new_col->name);
				return;  
			}
			current_col = current_col->next_col;
			printf("moving to next col on the list... (%s)\n", current_col->name);
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
	printf("interpreting %s\n", param1);
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
				printf("couldn't find var in list\n");
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
		printf("tbl name = %s\n", tbl_name);


		Db* db_search = lookup_db(db_name, db_head);
		Table* tbl_search = lookup_table(tbl_name, db_search);
		Column* col_search = lookup_column(col_name, tbl_search);
		if (col_search == NULL) {
			printf("Error in tracking down %s.%s.%s\n", db_name, tbl_name, col_name);
			return NULL;
		}
		else {
			printf("Found column %s\n", col_name);
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
		printf("list is empty\n");
		return 0;
	}
	else {
		printf("Summing %s\n", param1);
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
		printf("Averaging %s\n", param1);
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

void print_var(Var *var_pool, const char* create_arguments) {

	//printf("var pool = %p", var_pool);
	printf("args: %s\n", create_arguments);
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
 	    printf("variable name = %s\n", token);
    	//}// 

		Var* start = var_pool;

		//printf("var pool value = %p, start value = %p\n", var_pool, start);

		//printf("looking for var name %s\n", token);
		if (start == NULL)
			printf("var pool is empty - nothing to print\n");
		else do
		{
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
						for (int x = 0; x < results_to_print->count; x++) 
							log_info("%i\n", results_to_print->item[x]);
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
printf("looking for column %s in table %s\n", column_name, target_table->name);
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
	printf("comparing %s %li %s %li\n", start->var_name, strlen(start->var_name), var_name, strlen(var_name));
	if (strcmp(start->var_name, var_name) == 0) {
		printf("setting up int_list to point to the var_store\n");
		items_to_fetch = start->var_store;
		items_to_fetch_working = items_to_fetch;
//		if (results != NULL) {
//			while (results_to_print != NULL)
//			{
//				printf("number of results in this node = %i\n", results_to_print->count);
//				for (int x = 0; x < results_to_print->count; x++) {
//					int search_key = results_to_print->item[x];
//					while results_to_print = results_to_print->next;
//				}
//			};
//			printf("\n");
//		}
	};
	start = start->next;
} 
while (start != NULL);

printf("we're looking to fetch %i items\n", items_to_fetch_working->count);
int find_item = 0;
while (items_to_fetch_working != NULL) {
	for (unsigned int x = 0; x < items_to_fetch_working->count; x++) {
		find_item = items_to_fetch_working->item[x];
		printf("item to find is item # %i\n", find_item);
		printf("The current page ends at item # %i\n", (items_to_fetch_from->count + index_node));
		while ((items_to_fetch_from->count + index_node) < find_item) {
			printf("item doesn't seem to be in this page.\n");
			index_node += items_to_fetch_from->count;
			items_to_fetch_from = items_to_fetch_from->next;
		};
		printf("found item #%i - it is %i\n", find_item, items_to_fetch_from->item[find_item - index_node]);
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

printf("looking for column %s in table %s\n", column_name, table_name);
Column* target_column = lookup_column(column_name, target_table);
if (target_column == NULL)
{
	printf("Couldn't find the specified column in the db/table combo. Can't execute select.\n");
	return NULL;
};

printf("Assigning to %s\n", var_name);

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
			printf("table name is %s\n", new_table->name);
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
	printf("error checking create request...\n");
	fflush(stdout);
	if (db_head->empty_flag == 1) {				// what if there are NO dbs (i.e., list of present dbs is empty?)
		printf("db list is empty - creating a new one...\n");
		fflush(stdout);
		strcpy(db_head->name, db_name);	// copy the name
		db_head->tables = NULL;			// initialize other variables to indicate a totally empty db with
		db_head->tables_size = 0;		// no tables and pointing to nothing else.
		db_head->tables_capacity = 0;
		db_head->next_db = NULL;
		db_head->empty_flag = 0;
		printf("db name is %s\n", db_head->name);
		return db_head;
	}
	else {
		printf("db list is not empty - having to traverse...\n");
		struct Db *temp_db = lookup_db(db_name, db_head);		
		if (temp_db->next_db == NULL) {									// In this case, the wasn't found. So create it...
			printf("couldn't find decalred name in the existing list, so creating new...\n");
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
			printf("db name is %s\n", temp_db->name);
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

/*
Status insert_row(const char* db_name, Db* db_head, const char* table_name, int column_number, struct int_list* list, 
	Status *ret_status) {
	Status localStatus = *ret_status;
	if (list == NULL) {
		printf("list is empty; nothing to insert!\n");
		localStatus.code = OK;
		return localStatus;
	};

	struct Db* temp_db = lookup_db(db_name, db_head);		// Have to define db_head -- isn't quite ther

	struct Table* temp_table = lookup_table(table_name, temp_db);

	// Find end of column

	struct Column *temp_col = temp_table->columns;
	int x = 1;
	for (x = 1; x < column_number; x++)
		temp_col = temp_col->next_col;

	// Loop through each item on the int_list

	while (temp_col->data-> != NULL)
		temp_col->data = temp_col->data->next;

	struct int_list *y;

// come back to this

	while (list != NULL) {
		y = malloc(sizeof(int_list));
		y->item = list->item;
		//y->deleted_flag = 0;
		y->next = NULL;
		temp_col->data->next = y;
		list = list->next;
	};

ret_status = OK;
return *ret_status;
}
*/

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
			printf("Error in tracking down %s.%s\n", db_name, table_name);
			return NULL;
		}
		else {
			printf("Found %s.%s\n", db_name, table_name);
			// find the var and traverse list of items to delete
			Var* var_working = var_pool;
			int_list* delete_list;
			while (var_working)
			{
				// is the var name in the variable pool list?
				// if yes, this is dubbed the "delete_list"
				if (strcmp(var_working->var_name, var_name) == 0) {
					delete_list = var_working->var_store;
					printf("created delete column from variable $s\n", var_working->var_name);
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
					printf("Eliminating values\n");
					while (temp_col)
					{
						printf("...from column %s\n", temp_col->name);
					// in each iteration, find the data row and set to the presumptive deleted value
						int_list* working_int_list = temp_col->data;
						printf("item %i ", x);
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

printf("performing nested loop\n");
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
					printf("item #s: x = %i & y = %i. Comparison: %i and %i\n",(index_x + x), (index_y + y),
						values_1_working->item[x], values_2_working->item[y]);

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

printf("performing hash join\n");
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

	printf("hashing %i values\n", values->count);
	for (unsigned int x = 0; x < values->count; x++)
	{
		//printf("looking at item# %i", x);
		int_hash = values->item[x] % number_of_hash_buckets;
		//printf("hashes to %i ", int_hash);
		hash* hash_pointer = hash_table + sizeof(hash) * int_hash;
		if (hash_pointer->value == 0) {
			//printf("First item that hashes to %i ", int_hash);
			hash_pointer->value = values->item[x];
			printf("no colission @ hash pointer->value %i hashse to %i\n", hash_pointer->value, int_hash);
			hash_pointer->pos = x;
			hash* hash_pointer_new = malloc(sizeof(hash));
			hash_pointer_new->value = 0;
			hash_pointer_new->pos = x;
			hash_pointer_new->next = NULL;
			hash_pointer->next = hash_pointer_new;
		}
		else {
			printf("collision @ hash_pointer->value %i hashes to %i\n", hash_pointer->value, int_hash);
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


// put else clause here and code hash-join - does it have the same inputs??


/*  
int b_tree_engine(int argc, char *argv[]) {

// define C0 arrays

  unsigned fence_pointer_counter = 0;   // there is one set of fence pointers per node
  unsigned int counter = 0;             // number of items currently in C0 array
  int* array;                           // pointer to array of key-value pairs
  int flag = 0;                         // flag indicating whether value is found or not
  struct node* rootnode;                // pointer to the root node
  int key = 0;                          // working variable for key in key-value pair
  int value = 0;                        // working variable for value in key-value pair
  unsigned int x, y, z, c, d;           // working variable for loops, etc.
  int key_range_low = 0;                // working variable: low end of range for range function
  int key_range_high = 0;               // working variable: high end of range for range function
  char arg1;                            // working variables for lines read from work file.
  char arg2[80];
  int arg3;
  int arg4;
  int* reserve_pointer = malloc(sizeof(int*));
  int min_value = 0;
  int max_value = 0;
  
  // Timers - assuming 300,000 to begin with but scalable.
  clock_t* timer_array = malloc(300000 * sizeof(clock_t));
  int timer_array_index = 0;
  int timer_array_max = 300000;
  
  // Each level gets a reserved bloom filter
  struct bloom_array* bloom_filter = malloc(sizeof(struct bloom_array) * MAX_LEVELS);  
  char* workfile_pointer = malloc(80);
  char* log_pointer = malloc(80);

  // Error checking to ensure the right number of arguments are entered. None of these are
  // optional.
  if (argc != 10)
  {
    printf("This program requires 7 arguments - you provided %i:\n\n", argc);
    printf("Arg1     Number of cores\n");
    printf("Arg2     K-V pairs in main memory buffer (L0)\n");
    printf("Arg3     K-V pairs in each disk-stored node\n");
    printf("Arg4     Fan out\n");
    printf("Arg5     Number of LSM tree levels\n");
    printf("Arg6     Location of (input) workfile\n");
    printf("Arg7     Location of (output) logfile\n");
    printf("Arg8     Bloom Filters Activated (1=yes 0=no)?\n");
    printf("Arg9     Fence Pointers Activated (1=yes 0=no)?\n");
    exit(0);
  }
  else
  {
    int cores = atoi(argv[1]);
    printf("Cores = %i\n", cores);
    C0_SIZE = atoi(argv[2]);
    printf("C0 Size = %i\n", C0_SIZE);
    NODE_SIZE = atoi(argv[3]);
    printf("Node Size = %i\n", NODE_SIZE);
    FAN_OUT = atoi(argv[4]);
    printf("Fan Out = %i\n", FAN_OUT);
    MAX_LEVELS = atoi(argv[5]);
    printf("Max LSM Tree Levels on Disk = %i\n", MAX_LEVELS);
    workfile_pointer = argv[6];   // "/home/ubuntu/workspace/project0/project0-hashtable/workfile.txt"
    printf("Workfile path = %s\n", workfile_pointer);
    log_pointer = argv[7];        // "/home/ubuntu/workspace/project0/logfile"
    printf("Logfile path = %s\n", log_pointer);
    bloom_filters_activated = atoi(argv[8]);
    printf("Bloom filters activated = %i\n", bloom_filters_activated);
    fence_pointers_activated = atoi(argv[9]);
    printf("Fence pointers activated = %i\n", fence_pointers_activated);
  };

  // Clock starts now.
  clock_t start_time = clock();
  
  // The Occupancy Matrix indicates the number of storage nodes available at each level (dimension
  // [level][0]) and the number of those currently occupied.
  // column 0 = capacity of level x
  // column 1 = occupied nodes on level x

  unsigned int occupancy_matrix[MAX_LEVELS][2];
  for (x = 0; x < MAX_LEVELS; x++)
  {
    occupancy_matrix[x][0] = pow(FAN_OUT, x); 
    occupancy_matrix[x][1] = 0;
    printf("Occ matrix %i %i %i\n", x, occupancy_matrix[x][0], occupancy_matrix[x][1]);
  };
  
// create and populate an empty C0 array

  array = (int*)malloc(C0_SIZE * 2 * sizeof(int));    // allocate integers to array   *F

// create and populate an empty root node

  struct node newnode;
  rootnode = &newnode;
  rootnode->array = malloc(NODE_SIZE * 2 * sizeof(int)); // *F

// This is a reserve node where values are temporarily placed until last point of merge

  struct node* reserve_node;
  reserve_node = malloc(sizeof(struct node));                 // *F
  reserve_node->array = malloc(sizeof(int) * NODE_SIZE * 2 + 4);  // *F

// initialize 50,000 fence pointers (just to pick a number)

  struct fence_pointer *fence = malloc(sizeof(struct fence_pointer) * 50000);
  for (x=0; x < 50000; x++)              // set all max and min references in fence = 0
  {
    fence[x].max_value = 0;
    fence[x].min_value = 0;
    fence[x].counter = 0;
    fence[x].location = NULL;
  };
  fence[0].location = rootnode;         // set up ref to array for new node

// initialize bloom filter

  for (x = 0; x < (unsigned int)MAX_LEVELS; x++)
  {
    bloom_filter[x].count = NODE_SIZE * pow(FAN_OUT, x);
    bloom_filter[x].element = malloc (ceil ( NODE_SIZE * pow(FAN_OUT, x) * sizeof(unsigned int) / 32 ) + 8); 
    if (bloom_filter[x].element != NULL)
      printf("Successfully allocated for bloom filter %i\n", x);// *F
  };
  
// Open workfile

  FILE *fp;
  fp=fopen(workfile_pointer,"r");
  if (fp == NULL)
    exit(-1);

// Initialize write files

  FILE *level_files[MAX_LEVELS];
  for(int i = 0; i < MAX_LEVELS; i++) 
  {
    char filename[50];
    sprintf(filename, "/home/ubuntu/workspace/project0/level%03d", i);
    level_files[i] = fopen(filename, "w");
  };
  FILE *logfile = fopen(log_pointer, "w");
  
// Set up loop to read workfile

while(fscanf(fp, "%c %s %i\n", &arg1, arg2, &arg3) != EOF)
{
// Note second operand is a string to support the load function, which requires a file path.

// "Put" procedure - if scanned line is a put command, key and value are second and third operands.
// Search C0 for key and replace value there if found. If not found, then put it in the array.
// If can't fit into the array because out of of space, flush a segment to make space and then append to array structure 


  if (arg1 == 'p')
  {
    key = atoi(arg2);
    value = arg3;

// loop searches for pre-existing key and replaces value if found

    flag = 0;                       // flag of 0 means key hasn't been found (yet).
    for (x = 0; x < counter; x++)
    {
      if (array[x * 2] == key)
      {
        array[x * 2 + 1] = value;
        flag = 1;
      }
    };


    if (flag == 0)
    {
      if (counter == C0_SIZE) // Test to see if C0 completely filled up.
      {
        // if C0 filled, flush K-V C1, reduce counter by size of C0,
        // then add new value to C0.
        fence[0].counter = C0_SIZE;
        reserve_pointer = array + (C0_SIZE - NODE_SIZE) * 2;
        max_value = *reserve_pointer;
        min_value = *reserve_pointer;
        
        for (y = 0; y < NODE_SIZE; y++)
        {
          reserve_node->array[y * 2] = *reserve_pointer;
          if (*reserve_pointer > max_value)
            max_value = *reserve_pointer;
          if (*reserve_pointer < min_value)
            min_value = *reserve_pointer;
          reserve_pointer += 1;
          
          reserve_node->array[y * 2 + 1] = *reserve_pointer;
          reserve_pointer += 1;

        };
        flush(fence, occupancy_matrix, fence_pointer_counter, reserve_node, max_value, min_value, 
          level_files, bloom_filter, logfile);
        for (y = (C0_SIZE - NODE_SIZE); y < C0_SIZE; y++)
        {
          array[y * 2] = 0;
          array[y * 2 + 1] = 0;
        };

        // Place new key-value pair in the now-cleared third array segment.

        array[(C0_SIZE - NODE_SIZE) * 2] = key;
        array[(C0_SIZE - NODE_SIZE) * 2 + 1] = value;
        counter = C0_SIZE - NODE_SIZE + 1;
      }
    else
    {
      array[counter*2] = key;
      array[counter*2 + 1] = value;
      flag = 1;
      counter++;
    }
  }
  *(timer_array + timer_array_index) = clock() - start_time;  
  double msec = 1000 * (double)*(timer_array + timer_array_index) / CLOCKS_PER_SEC;
  fprintf(logfile, "p %lf\n", msec);
  timer_array_index++;
  if (timer_array_index > (0.9 * timer_array_max)) 
  {
    timer_array_max *= 2;
    timer_array = realloc(timer_array, timer_array_max * sizeof(clock_t));
  };
}

// Logic for get
// Search C0 first

  else if (arg1 == 'g')
    {
      flag = 0;
      int arg2_i = atoi(arg2);    // arg2 is a string.
//    printf("Bloom filter count: %u\n", bloom_filter->count);
      get_key(arg2_i, array, logfile, occupancy_matrix, bloom_filter, fence, counter);
      *(timer_array + timer_array_index) = clock() - start_time;   // all the real work is in get_key fxn.
      double msec = 1000 * (double)*(timer_array + timer_array_index) / CLOCKS_PER_SEC;
      fprintf(logfile, "g %lf\n", msec);
      timer_array_index++;
    }
  else if (arg1 == 'r')
    {
      flag = 0; 
      key_range_low = atoi(arg2);
      key_range_high = arg3;
      // create and initialize a range node (first of a linked list)
      struct range_ll* node = (struct range_ll*)malloc(sizeof(struct range_ll*));   // F*
      node->key = 0;
      node->value = 0;
      node->next = NULL;
      // establish this node as the head of the lsit.
      struct range_ll* head = node;
      // loop through buffer - if elements in buffer fall within high-low range, add.
      for (y = 0; y < counter; y++)
      {
        if ((array[y * 2] >= key_range_low) && (array[y * 2] < key_range_high))
        {
          // starting at head, traverse tree
          struct range_ll* test = head;
          struct range_ll* last = head;
          // move to next node if (i) next node exists, (ii) key in next node > key in array, and
          // (iii) key in present key <
          while (test->next != NULL && (test->next)-> key > array[y * 2] && (test->key < array[y * 2]))
          {
            last = test;
            test = test->next;
          };
          if (test->key == array[y * 2])
            continue;
          else
          {
            node->key = array[y * 2];
            node->value = array[y * 2 + 1];
            last->next = node;
            node->next = test;
            if (array[y * 2 + 1] != INT_MAX)
            {
              printf("%i:%i ", array[y * 2], array[y * 2 + 1]);
              flag = 1;
            };
            struct range_ll* new_node = (struct range_ll*)malloc(sizeof(struct range_ll*));   // F*
            new_node->key = 0;
            new_node->value = 0;
            new_node->next = test->next;
            node->next = new_node;
            node = new_node;
          };
        };
      };
      node = head;
      for (y = 0; y < (unsigned int)MAX_LEVELS; y++)
      {
        // start searching at the head of the linked list
        struct range_ll* test = head;
        struct range_ll* last = head;
        // is there anything on this level? if not, skip it
        if (occupancy_matrix[y][1] == 0)
          continue;
        char filename[50];
        int a, b;
        // once an occupied level is found, open file for read
        sprintf(filename, "../src/level%03d", y);
        FILE* level_file = fopen(filename, "rb");
        flag = 0;
        // read a byte - if there's something to read (i.e., result not 0)...
        while(fread(&a, sizeof(int), 1, level_file) != 0)
        {
          // read next byte
          fread(&b, sizeof(int), 1, level_file);
          // is the key read (the first byte) within the low high range?
          if (a >= key_range_low && a < key_range_high)
          {
            // is it deleted? if yes, skip to next read - won't record this one
            if (b == INT_MAX)
              continue;
            else
            {
              // if not deleted, and within range, push through tree to find next node as long as three
              // conditions hold: (1) there IS a next node, (2) the key in the next node > key,
              // (3) the key in the next node < key
              while (test->next != NULL && (test->next)->key > a && (test->key < a))
              {
                last = test;
                test = test->next;
              };
              // once the loop is exited
              node = head;
              
              if (node->key == a)
                continue;
              else
              {
                node->key = a;
                node->value = b;
                printf("%i:%i \n", a, b);
                fflush(logfile);
                flag = 1;
                struct range_ll* new_node = (struct range_ll*)malloc(sizeof(struct range_ll*));
                new_node->key = 0;
                new_node->value = 0;
                new_node->next = NULL;
                node->next = new_node;
                node = new_node;
              };
            };
          };
        };
        printf("\n");
        fclose(level_file);
      };
    if (flag == 0) printf("\n");
    }
  else if (arg1 == 'd')
    {
      int arg2_i = atoi(arg2);
      flag = 0;
      for (x = 0; x < C0_SIZE; x++)
      {
        if (array[x * 2] == arg2_i)
        {
          flag = 1;
          fprintf(logfile, "Deleting from C0 %i:%i\n ", array[x * 2], array[x * 2 + 1]);
          deleted_count++;                  //  added for testing. remove later.
          printf("d# %i ", deleted_count);  // added for testing. remove later.
          array[x * 2 + 1] = INT_MAX;
          fflush(stdout);
          flag = 1;
          break;
        };
      };
      if (flag == 0)
      {
        char filename[50];
        // find last level that contains nodes, place in x.
        for (x = MAX_LEVELS; x > 0; x--)
          if (occupancy_matrix[x - 1][1] != 0)
            break;
          else
            continue;
        // initialize
        z = 0;
        for (y = 0; (y < (unsigned int)MAX_LEVELS && flag == 0); y++)
        {
          int fence_index = 0;
          if (bloom_check(arg2_i, &bloom_filter[y]) == 0)
          {
            fence_index += occupancy_matrix[y][0];
            continue;
          }
          else
          {
            sprintf(filename, "../src/level%03d", y);
            FILE* level_file = fopen(filename, "rb+");
            int a, b;
            for (c = 0; (c < pow(FAN_OUT, y) && flag == 0); c++)
            {
              if (fence_pointers_activated == 1 && (arg2_i >= fence[fence_index + c].min_value && arg2_i <= fence[fence_index + c].max_value))
                fseek(level_file, NODE_SIZE * 8 * c, SEEK_SET);
              for (d = 0; d < (fence[fence_index + c].counter * (fence_pointers_activated == 1) + NODE_SIZE * (fence_pointers_activated == 0)); d++)
              {
                fread(&a, sizeof(int), 1, level_file);
                fread(&b, sizeof(int), 1, level_file);
                if (a == arg2_i)
                {
                  b = INT_MAX;
                  fseek(level_file, -4, SEEK_CUR);
                  fwrite(&b, sizeof(int), 1, level_file);
                  fflush(level_file);
                  flag = 1;
                  break;
                }
              };
            if (flag == 1)
              break;
            };
          };
        };
      };
    }
  else if (arg1 == 's')
    {
      printf("\n\nAs of now, Occupancy matrix looks like:\n");
      for(x = 0; x < MAX_LEVELS; x++)
        printf("%i %i | ", occupancy_matrix[x][0], occupancy_matrix[x][1]);

      int a, b;
      printf("\nTree Statistics\n\n");
      printf("Number of Keys in Each Level of the Tree\n");
      printf("Main Memory: %u\n", counter);
      printf("Main Memory K-V Pairs:\n");
      for (x = 0; x < counter; x++)
        if (array[x * 2 + 1] != -1)
          printf("[%i]:[%i] ", array[x * 2], array[x * 2 + 1]);
          
      c = 0;
      for (x = 0; x < MAX_LEVELS; x++)
      {
        char filename[50];
        sprintf(filename, "../src/level%03d", x);
        FILE* level_file = fopen(filename, "rb");
        for (d = 0; d < occupancy_matrix[x][1]; d++)
        { 
          printf("\nLevel %u\nNode %u\n", x, c);
          printf("Counter = %u\n", fence[c].counter);
          printf("Min = %i Max = %i\n", fence[c].min_value, fence[c].max_value);
          for (y = 0; y < NODE_SIZE; y++)
          {
            fread(&a, sizeof(int), 1, level_file);
            fread(&b, sizeof(int), 1, level_file);
            if (b != -1)
              printf("[%i]:[%i] ", a, b);
          };
          c++;
        printf("\n");
        };
        c += occupancy_matrix[x][0] - occupancy_matrix[x][1];
        printf("\n");
      };
    }
  else if (arg1 == 'l')
    {
      fp = fopen(arg2,"rb");
      if (fp == NULL)
      {
        printf("Open File Failed.\n");
        exit(-1);
      };
      while(!feof(fp))                     // As long as EOF hasn't been reached...
      {
        fread(&arg3, sizeof(int), 1, fp);  // Read key and value.
        fread(&arg4, sizeof(int), 1, fp);
        if (feof(fp)) break;               // DOES THIS NEED TO BE HERE?
        key = arg3;
        value = arg4;

// double nested loop searches for pre-existing key and replaces value if found

        flag = 0;
        for (x = 0; x <= counter; x++)    // Loop checks for pre-existing instance in C0
        {                                 // of key just read
          if (array[x * 2] == key)        // If found, replace associated value in C0 with
          {                               // value just read
            array[x * 2 + 1] = value;
            flag = 1;                     // Set flag to show put iteration is done.
          }
        };
        if (flag == 0)                    // If flag isn't set then value wasn't found.
        {
          if (counter == C0_SIZE)         // Is C0 completely full?
          {
            // if C0 filled, move a NODE-SIZE number of k-v pairs to reservoir, then
            // add new value to C0 where just moved data used to be.
            reserve_pointer = array + (C0_SIZE - NODE_SIZE) * 2; // Reserve pointer set here.
            max_value = *reserve_pointer;     // We track max and min values of the reservoir
            min_value = *reserve_pointer;     // because this is the soruce of the fence pointer data
            for (y = 0; y < NODE_SIZE; y++)
            {
              reserve_node->array[y * 2] = *reserve_pointer; // Reserve node array is where reserve info is stored
              if (*reserve_pointer > max_value)              // Update max and min
                max_value = *reserve_pointer;
              if (*reserve_pointer < min_value)
                min_value = *reserve_pointer;
              reserve_pointer += 1;                         // Advance reserve pointer
              reserve_node->array[y * 2 + 1] = *reserve_pointer;
              reserve_pointer += 1;                         // Advance reserve pointer

            };
            flush(fence, occupancy_matrix, fence_pointer_counter, reserve_node, max_value, min_value, level_files, bloom_filter, logfile);
          for (y = C0_SIZE; y < (C0_SIZE + NODE_SIZE); y++)
          {
            array[y * 2] = 0;         // Empty out C0 to get ready for
            array[y * 2 + 1] = 0;     // more data!
          };

          // Place new key-value pair in the now-cleared third array segment.
          array[(C0_SIZE - NODE_SIZE) * 2] = key;
          array[(C0_SIZE - NODE_SIZE) * 2 + 1] = value;
          counter = C0_SIZE - NODE_SIZE + 1;
        }
        else
        {
        array[counter*2] = key;           // This code handles the case where C0 is scanned for a given
        array[counter*2 + 1] = value;     // key and it's already there and needs to be replaced.
        flag = 1;
        counter++;
        }
      }

      *(timer_array + timer_array_index) = clock() - start_time;  // ....Aaaand TIME! Computation of time ends here
      double msec = 1000 * (double) *(timer_array + timer_array_index) / CLOCKS_PER_SEC;
      // Units measurement is millisconds.
      timer_array_index++;                                          // Advance time array
      fprintf(logfile, "timer %i %f | ", timer_array_index, msec);  // Log it.
    };
  }
}
}

int flush (struct fence_pointer* fence, unsigned int occupancy_array[][2],
  unsigned int fence_pointer_counter, struct node* reserve, int max_value,
  int min_value, FILE* level_files[], struct bloom_array* bloom_filter,
  FILE* logfile) 
{

// Inputs:      Pointer to the reserve array to be flushed
//              Pointer to the fence pointers
//              Occupancy array
//              File pointers (since they are flushed to various disk files)
//              Bloom filter array for level l since has to be reset when level l is modified.

fence_pointer_counter +=1;    // Flushing a new node requires addition to fence pointer counter
unsigned int x = 0;           // Workin variables.
unsigned int y = 0;
unsigned int z = 0;
unsigned int first_node_at_llf;           // "llf" refers to last level to flush.
unsigned int first_node_at_llf_plus_one;  // "llf plus one" is the level to which the llf will be flushed.
struct node* pointer_to_first_node_at_llf;

unsigned int last_level_in_lsm_tree;      // last populated level in the tree.
unsigned int count_of_elements_in_first_node_at_llf;

unsigned int llf;

int flush_flag = 1;

while (flush_flag == 1)
{
llf = 0;

quicksort(reserve->array, 0, NODE_SIZE - 1);    // call sorting algorithm to sort reserve array.
                                                // having reserve and all disk levels sorted is
                                                // important because it saves time when searching for
                                                // stored data later.

llf = 0;                                        // find lowest level to flush
y = occupancy_array[0][1] + 1;                  // set y = to current occupancy of level 0 plus the reserve node.
for (x = 0; x < MAX_LEVELS; x++)                // loop though all levels of the tree
{
  if (y > occupancy_array[x][0])                // if y exceeds total capacity of a level, THAT is the llf
  {
    llf = x;
    if (llf == (MAX_LEVELS - 1))                // if the system runs out of levels, increase the max variable 
    {                                           // and expand the occupancy array 
      MAX_LEVELS++;                             // IS MAX CAPACITY OF THIS NEW LEVEL ASSIGNED SOMEWHERE?
//      realloc(occupancy_array, sizeof(unsigned int) * MAX_LEVELS * 2);
    };
    y = occupancy_array[x + 1][1] + occupancy_array[x][0]; // set y = amt of next level that is filled plus
  }                                                        // occpancy of current level
  else
    break;                                      // if y doesn't exceed capacity, end here because there's no need to
};                                              // flush this level.

// flush the lowest level to the next level

// allocate enough workspace to flush to next level.

z = (unsigned int)pow(FAN_OUT, llf + 1);
struct node workspace;              // create a new node called workspace with enough space for ALL OF level llf + 1
workspace.array = (int*)malloc((llf + 1) * NODE_SIZE * 2 * z * sizeof(int));

x = occupancy_array[llf][0];        // x = number of nodes possible in level llf
y = occupancy_array[llf + 1][0];    // y = number of nodes possible in level llf + 1
x = occupancy_array[llf][1];        // 
y = occupancy_array[llf + 1][1];

first_node_at_llf = 0;              // calculate fence pointer number for first node in level llf.
for (unsigned int c = 0; c < llf; c++)
{
  first_node_at_llf += occupancy_array[c][0];
};

// fence[first_node_at_llf].counter is the number of items in node first_node_at_llf

if ((llf == 0) && (fence[0].counter == 0)) // if llf = 0, pointer_to_first_node_at_llf points to the L0 reserve array.
{
  pointer_to_first_node_at_llf = reserve;
}
else if ((occupancy_array[llf + 1][1] != 0))
{
  pointer_to_first_node_at_llf = (fence[first_node_at_llf].location);
}
else
{
  pointer_to_first_node_at_llf = (fence[first_node_at_llf].location);
};

last_level_in_lsm_tree = 0;
for (z = MAX_LEVELS - 1; z > 0; z--)             // find highest occupied level.
{
  if (occupancy_array[z][1] > 0)
  {
    last_level_in_lsm_tree = z;     //
    break;
  };
};

  z = 0;                                          // calculate nodes in llf.
  first_node_at_llf_plus_one = 0;
  for (unsigned int c = 0; c < (llf + 1); c++)
  {
    first_node_at_llf_plus_one += occupancy_array[c][0];                 // calculate z = first node of llf+1
  };

if (first_node_at_llf != 0)
  count_of_elements_in_first_node_at_llf = fence[first_node_at_llf].counter;           // z_count = count of elements in
else                                    // given node.
  count_of_elements_in_first_node_at_llf = NODE_SIZE * 2;

// This code is gnarly and requires explanation.
// There are three scenarios the flush routine handles.
//
// Scenario 1: L0 (first level on disk) is empty. C0 (main memory array) is full and needs
// to be dumped into L0.

  
if (llf == 0 && occupancy_array[0][1] == 0)
{
  flush_flag = 0;   // since c0 is moved last, once it is moved, set flush flag to 0
  workspace.array = (int*)malloc(NODE_SIZE * 2 * sizeof(int)); // create a temporary workspace = size of node
  level_files[0] = fopen("../src/level000", "wb"); // open file level000 (L0)
  bloom_clear(&bloom_filter[0], 0);

  for (x = 0; x < NODE_SIZE; x++)
  {
    workspace.array[x * 2] = reserve->array[x * 2];   // copy reserve area into work area and write to disk.
    fwrite(&reserve->array[x * 2], sizeof(int), 1, level_files[0]);
    if (bloom_filters_activated == 1)
      bloom_set(workspace.array[x * 2], &bloom_filter[0]);
    workspace.array[x * 2 + 1] = reserve->array[x * 2 + 1];
    fwrite(&reserve->array[x * 2 + 1], sizeof(int), 1, level_files[0]);
  };
  fclose(level_files[0]);               // close level000 file.
  occupancy_array[0][1] = 1;            // note L0 as occupied.
  struct node* newnode = (struct node*)malloc(sizeof(struct node*));        // create a new node.
  newnode->array = (int*)malloc((llf + 1) * NODE_SIZE * 2 * sizeof(int));   // create an array of size of llf+1
  newnode->array = workspace.array;                                         // point new node array to workspace.
  fence[0].location = newnode;           // fence pointer location points to new node.
  fence[0].counter = NODE_SIZE * 2;
  fence[0].max_value = max_value;       // copy the max and min values into node 0.
  fence[0].min_value = min_value;
 }                                      // IS THIS NEW NODE AND LOCATION INFO FOR THE FENCE NECESSARY?
// Scenario 2: llf is filled to capacity (not partially) and the level below that, llf+1, has nothing in it.
// This case is relatively simple because all you have to do is move the nodes, you don't have to compute anything.
 else if (occupancy_array[llf][1] == occupancy_array[llf][0] && occupancy_array[llf + 1][1] == 0)    // move only, nothing to compare
 {
   char filename[50];                                                       
   sprintf(filename, "../src/level%03d", llf + 1); // open file for llf + 1
   level_files[llf + 1] = fopen(filename, "wb");   // open file to write
   sprintf(filename, "../src/level%03d", llf);     // ... and open file for llf.
   level_files[llf] = fopen(filename, "rb");       
   fseek(level_files[llf], 0, SEEK_END);          // Measure size of llf file.
   long lengthOfFile = ftell(level_files[llf]);
   fseek(level_files[llf], 0, SEEK_SET);
   long currPos = 0;
   int a, b;
   bloom_clear(&bloom_filter[llf], 0);            // empty out bloom filter for lev llf
   if (llf < (MAX_LEVELS - 1))
   {
//   printf("llf = %i\n", llf);
     bloom_clear(&bloom_filter[llf + 1], 1);
   };
   int amt_written = 0;
   while (lengthOfFile != currPos)
   {
     fread(&a, sizeof(int), 1, level_files[llf]);        // read a and b from llf file,
     fread(&b, sizeof(int), 1, level_files[llf]);        // write a and b to llf+1 file.
     fwrite(&a, sizeof(int), 1, level_files[llf + 1]);   // ... rinse and repeat until end of llf file.
     if (bloom_filters_activated == 1)
      bloom_set(a, &bloom_filter[llf + 1]);
     fwrite(&b, sizeof(int), 1, level_files[llf + 1]);
     amt_written += 2;
     currPos += 8;
   };
   fclose(level_files[llf + 1]);
   fclose(level_files[llf]);
   int c = 0;
   for (y = 0; y < llf; y++)              // Calculate the first node of the llf. = c
     c += occupancy_array[y][0];
   //c--;
   for (int d = 0; d < pow(FAN_OUT, llf); d++)      // loop through all nodes of llf, = d
    {

     fence[c + d].counter = 0;                    // c + d refers to the dth node of level c.
     fence[c + d + occupancy_array[llf][0]].counter = NODE_SIZE * 2;    // clear out dth node of level c,
     fence[c + d + occupancy_array[llf][0]].max_value = fence[c + d].max_value;   // populate counter/max/min of dth node
     fence[c + d].max_value = 0;                                                  // of level c+1.
     fence[c + d + occupancy_array[llf][0]].min_value = fence[c + d].min_value;
     fence[c + d].min_value = 0;
    };
   occupancy_array[llf + 1][1] = occupancy_array[llf][1];
   occupancy_array[llf][1] = 0;
 }
 else 
 // Scenario 3: This is the most interesting, in which llf is full and llf is *partially* full
 {
    char filename_1[50], filename_2[50], filename_3[50];                                                            // open file
    sprintf(filename_1, "../src/level%03d", llf);
    level_files[llf] = fopen(filename_1, "rb");       // open llf file to read
    sprintf(filename_2, "../src/level%03d", llf + 1);
    level_files[llf + 1] = fopen(filename_2, "rb");   // open llf file to read
    sprintf(filename_3, "../src/merger_workfile");
    FILE* merger_workfile = fopen(filename_3, "wb");

    int a, b, c, d;
    unsigned int node_counter = first_node_at_llf_plus_one;
    fence[node_counter].counter = 0;
    int f1_count = 0, f2_count = 0, f3_count = 0;       // initialize file position counters = 0.
    int flag = 0;                                       // flag=0 indicates neither llf nor llf+1 files are at end.

    fread(&a, sizeof(int), 1, level_files[llf]);        // read one k-v pair, a and b, from llf file.
    fread(&b, sizeof(int), 1, level_files[llf]);
    f1_count++;                                         // advance llf file position counter.
    fread(&c, sizeof(int), 1, level_files[llf + 1]);    // read one k-v pair, c and d, from llf+1 file.
    fread(&d, sizeof(int), 1, level_files[llf + 1]);
    f2_count++;                                         // advance llf+1 file position counter.
    fence[node_counter].max_value = INT_MIN;// ( ((a) > (c)) ? (a) : (c) );
    fence[node_counter].min_value = INT_MAX;//( ((a) > (c)) ? (c) : (a) );
    while (flag == 0)
    {
      if (a <= c)                                     // is key just read from llf < key read from llf+1?
      {
        fwrite(&a, sizeof(int), 1, merger_workfile);  // IF YES, write that one along with value, in merger file.
        if (bloom_filters_activated == 1)
          bloom_set(a, &bloom_filter[llf + 1]);
        fwrite(&b, sizeof(int), 1, merger_workfile);
        fence[node_counter].counter+=2;
        if (fence[node_counter].max_value < a)
          fence[node_counter].max_value = a;
        if (fence[node_counter].min_value > a)
          fence[node_counter].min_value = a;
        f3_count++;                                   // advance merger file counter.
        if (fread(&a, sizeof(int), 1, level_files[llf]) != 0) // have to change this
        {                                             // read next a and b from llf file if ! eof (llf)
          fread(&b, sizeof(int), 1, level_files[llf]);
          f1_count++;                                 // advance llf file position counter.
        }
        else
        {
          fwrite(&c, sizeof(int), 1, merger_workfile);
          if (bloom_filters_activated == 1)
            bloom_set(c, &bloom_filter[llf + 1]);
          fwrite(&d, sizeof(int), 1, merger_workfile);
          fence[node_counter].counter+=2;
          if (fence[node_counter].max_value < c)
            fence[node_counter].max_value = c;
          if (fence[node_counter].min_value > c)
            fence[node_counter].min_value = c;

          f3_count++;
          flag = 1;                                 // set flag = 1, which will end while loop.
                                                      // The reason for this is that if the llf file is done,
                                                      // there's no longer any reason to have the "horse race"
                                                      // between the files - code below where flag=1 just cranks
                                                      // through llf+1 file
        }
      }
      else
      {
        fwrite(&c, sizeof(int), 1, merger_workfile);
        if (bloom_filters_activated == 1)
          bloom_set(c, &bloom_filter[llf + 1]);
        fwrite(&d, sizeof(int), 1, merger_workfile);
        fence[node_counter].counter+=2;
        if (fence[node_counter].max_value < c)
          fence[node_counter].max_value = c;
        if (fence[node_counter].min_value > c)
          fence[node_counter].min_value = c;
        f3_count++;
        if (fread(&c, sizeof(int), 1, level_files[llf + 1]) != 0) // change
        {
          fread(&d, sizeof(int), 1, level_files[llf + 1]);
          f2_count++;
        }
        else
        {
          fwrite(&a, sizeof(int), 1, merger_workfile);
          if (bloom_filters_activated == 1)
            bloom_set(a, &bloom_filter[llf + 1]);
          fwrite(&b, sizeof(int), 1, merger_workfile);
          fence[node_counter].counter+=2;
          if (fence[node_counter].max_value < a)
            fence[node_counter].max_value = a;
          if (fence[node_counter].min_value > a)
            fence[node_counter].min_value = a;
          f3_count++;
          flag = 2;
        }
      };
      if ((f3_count) % NODE_SIZE == 0)
      {
        node_counter++;
        fence[node_counter].counter = 0;
      };
    };
    if (flag == 1)                // llf ended first
      while (fread(&c, sizeof(int), 1, level_files[llf + 1]) != 0)
      {
        fread(&d, sizeof(int), 1, level_files[llf + 1]);
        f2_count++;
        fwrite(&c, sizeof(int), 1, merger_workfile);
        if (bloom_filters_activated == 1)
          bloom_set(c, (bloom_filter + (llf + 1)));
//      bloom_set(c, llf + 1, bloom_filter[llf + 1]);
        fwrite(&d, sizeof(int), 1, merger_workfile);
        fence[node_counter].counter+=2;
        if (fence[node_counter].max_value < c)
          fence[node_counter].max_value = c;
        if (fence[node_counter].min_value > c)
          fence[node_counter].min_value = c;
        f3_count++;
        if (f3_count % NODE_SIZE == 0)
        {
          node_counter++;
          fence[node_counter].counter = NODE_SIZE * 2;
          fence[node_counter].counter = 0;
          fence[node_counter].max_value =  ( ((a) > (c)) ? (a) : (c) );
          fence[node_counter].min_value = ( ((a) > (c)) ? (c) : (a) );
        };
      };
    if (flag == 2)
      while (fread(&a, sizeof(int), 1, level_files[llf]) != 0)
      {
        fread(&b, sizeof(int), 1, level_files[llf]);
        f1_count++;
        fwrite(&a, sizeof(int), 1, merger_workfile);
        if (bloom_filters_activated == 1)
          bloom_set(a, &bloom_filter[llf + 1]);
        fwrite(&b, sizeof(int), 1, merger_workfile);
        fence[node_counter].counter+=2;
        if (fence[node_counter].max_value < a)
          fence[node_counter].max_value = a;
        if (fence[node_counter].min_value > a)
          fence[node_counter].min_value = a;
        f3_count++;
        if (f3_count % NODE_SIZE == 0)
        {
          fence[node_counter].counter = NODE_SIZE * 2;
          node_counter++;
          fence[node_counter].counter = 0;
          fence[node_counter].max_value =  ( ((a) > (c)) ? (a) : (c) );
          fence[node_counter].min_value = ( ((a) > (c)) ? (c) : (a) );
        };
      };
    fclose(level_files[llf]);
    fclose(level_files[llf + 1]);
    fclose(merger_workfile);
    
    int ret = remove(filename_2);
    ret = rename(filename_3, filename_2);
    if (ret != 0) 
//  {
//    printf("ending weirdly\n");
      exit(-1);
//  };
  occupancy_array[llf + 1][1] += occupancy_array[llf][1];
  occupancy_array[llf][1] = 0;
  };
};
return 0;
}
// refresh all other fence pointers

void quicksort(int A[], int lo, int hi)
{
  int p = 0;
  int* lo_ptr = &lo;
  int* hi_ptr = &hi;
  if (lo < hi)
  {
    p = partition(A, lo_ptr, hi_ptr);
    quicksort(A, lo, p - 1);
    quicksort(A, p + 1, hi);
  };
}

int partition(int A[], int* lo, int* hi)
{
  int j = 0;
  int k = 0;
  int x = *lo;
  int y = *hi + 1;
  int pivot = A[*lo * 2];

  while (1)
  {
    do ++x; while (A[x * 2] < pivot && x <= *hi);
    do --y; while (A[y * 2] > pivot);
    if (x >= y) break;
    j = A[x * 2];
    k = A[x * 2 + 1];
    A[x * 2] = A[y * 2];
    A[x * 2 + 1] = A[y * 2 + 1];
    A[y * 2] = j;
    A[y * 2 + 1] = k;
  }
  j = A[*lo * 2];
  k = A[*lo * 2 + 1];
  A[*lo * 2] = A[y * 2];
  A[*lo * 2 + 1] = A[y * 2 + 1];
  A[y * 2] = j;
  A[y * 2 + 1] = k;
  return (y);
}

unsigned int bloom_set(int key, struct bloom_array* bloom_filter)
{
  if (bloom_filters_activated == 0)
    return 0;
  int key_u = key;
  unsigned int bloom_hash = ((((((key_u >> 16) ^ key_u >> 16) ^ (key_u >> 16) ^ key_u) 
    * 0x45d9f3b >> 16) ^ (((key_u >> 16) ^ key_u >> 16) ^ (key_u >> 16) ^ key_u) * 0x45d9f3b) * 0x45d9f3b);  // http://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
  unsigned int divisor = 4294967295 / bloom_filter->count; // max 32 bit signed int
  unsigned int bit_to_fill = (bloom_hash / divisor);
//  bloom_filter->element[bit_to_fill / 32] |= (1 << (bit_to_fill % 32));
//printf("bf&%p ", (&(bloom_filter->element) + (bit_to_fill / 32)));
  *(bloom_filter->element + (bit_to_fill / 32)) |= (1 << (bit_to_fill % 32));
//printf("Set k %i, h %u, bit %u\n", key_u, bloom_hash, bit_to_fill);
//printf("s k %i b %u\n", key_u, bit_to_fill);
  return 1;
}

unsigned int bloom_check(int key, struct bloom_array* bloom_filter)
{
//printf("Bloom filters activated? %i  1=yes 0=no", bloom_filters_activated);
//printf("Bloom filter count: %u\n", bloom_filter->count);
  if (bloom_filters_activated == 0)
    return 0;
  int key_u = key;
  unsigned int bloom_hash = ((((((key_u >> 16) ^ key_u >> 16) ^ (key_u >> 16) ^ key_u) 
    * 0x45d9f3b >> 16) ^ (((key_u >> 16) ^ key_u >> 16) ^ (key_u >> 16) ^ key_u) * 0x45d9f3b) * 0x45d9f3b);  // http://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
//printf("C k %i, h %u, ct %u\n", key_u, bloom_hash, bloom_filter->count);
  unsigned int divisor = 4294967295 / bloom_filter->count; // max 32 bit signed int
  unsigned int bit_to_fill = (bloom_hash / divisor);
//printf("Check k %i, h %u, bit %u\n", key_u, bloom_hash, bit_to_fill);  
//printf("bf&%p ", (&(bloom_filter->element) + (bit_to_fill / 32)));
  if ((*(bloom_filter->element + (bit_to_fill / 32)) & (1 << (bit_to_fill % 32))) != 0)
  {
    return 1;
  }
  else
  {
    return 0;
  };
}

unsigned int bloom_clear(struct bloom_array* bloom_filter, int level)
{
//printf("Clearing bloom filter level count = %i\n", bloom_filter[level].count);
  if (bloom_filters_activated == 0)
    return 0;
  for (int x = 0; x < bloom_filter->count; x++)
  {
    *(bloom_filter->element + (x / 32)) &= ~(1 << (x % 32));
  };
  return 1;
}

int get_key(int arg2_i, int* array, FILE* logfile, unsigned int occupancy_matrix[][2], struct bloom_array* bloom_filter, 
  struct fence_pointer fence[], int counter)
{
int a = 0, b = 0, x = 0, y = 0, z = 0, flag = 0, end_flag = 0, last_node = 0;
//printf("Search for %i ", arg2_i);
for (x = 0; x < counter; x++)
  {
    if (array[x * 2] == arg2_i && array[x * 2 + 1] != INT_MAX)
    {
      flag = 1;
      printf("%i\n L0", array[x * 2 + 1]);
      return array[x * 2 + 1];
    }
    else
    if (array[x * 2] == arg2_i)
    {
      printf("\n");
      return 0;
    };
  };

// if not found, search tree

if (flag == 0)
  {
    last_node = 0;
    for (x = MAX_LEVELS; x > 0; x--)     // find highest occupied level.  CHECK THIS LOOP
    {
      if (occupancy_matrix[x - 1][1] != 0)
        break;
    };
    int first_node_of_level = 0;
    int current_node_of_level = 0;
    for (y = 0; (y < x) && (flag == 0); y++) 
    {
      end_flag = 0;
//    printf("L%i ", y);
      if (bloom_filters_activated == 1 && bloom_check(arg2_i, &bloom_filter[y]) == 0)
      {
        first_node_of_level += pow(FAN_OUT, y);
        current_node_of_level = first_node_of_level;
        continue;
      }
      else
      {
        printf("L%i ",y);
        char filename[50];
        sprintf(filename, "/home/ubuntu/workspace/project0/level%03d", y);
        FILE* level_file = fopen(filename, "rb");
        fseek(level_file, 0, SEEK_SET);
        flag = 0;
        for (int q = 0; q < pow(FAN_OUT, y); q++)
        {
//        printf("N%i ", q);
          if (fence_pointers_activated == 0 || (arg2_i >= fence[current_node_of_level].min_value && arg2_i <= fence[current_node_of_level].max_value))   // value might be here, so scan array
          { 
            end_flag = 1;
            z = 0;
            for (z = 0; z < NODE_SIZE && flag == 0; z++)
            {
              fread(&a, sizeof(int), 1, level_file);
              fread(&b, sizeof(int), 1, level_file);
              if (arg2_i == a)
              {
                if (b == INT_MAX)
                {
                  flag = 1;
                  fprintf(logfile, "Deleted %i\n", arg2_i);
                  break;
                }
                else
                {
                  printf("%i\n", b);
                  flag = 1;
                };
              };
            };
          }
          else 
          {
            if (end_flag == 1)
              break;
            else
            {
              fseek(level_file, NODE_SIZE * 8, SEEK_CUR);
            };
          };
          if (flag == 1)
            break;
          current_node_of_level += 1;
        };
        fclose(level_file);
      };
    first_node_of_level += occupancy_matrix[y][0];
    current_node_of_level = first_node_of_level;
    };
  };  
  if (flag == 0)
  {
    printf("g not found\n");
    return 0;
  } 
  else
    return b;
}
*/