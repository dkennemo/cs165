#include "cs165_api.h"
#include <string.h>

// In this class, there will always be only one active database at a time
// NOTE: Per SI, this is not the case for 2018; more than one database may be
// open at a time.
Db *current_db;

/* 
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Status create_column(Table *table, char *name, Status *ret_status) {
	printf("CREATE COLUMN ROUTINE. \nsearching for the first empty column in %s (and also checking avail of name %s)\n", table, name);
	Column* current_col = table->columns;
	if (current_col == NULL) {
		printf("hmm.. didn't initialize with any columns!\n");
		ret_status->code = OK;
		return *ret_status;
	}
	else while(1)
	{
		if (strcmp(name, current_col->name) == 0)
		{
			printf("you already have a column named %s! try again.\n", name);
			ret_status->code = OK;
			return *ret_status;
		}
		else if (current_col->name[0] == '\0')
		{
			printf("adding column name %s\n", name);
			strcpy(current_col->name, name);
			ret_status->code = OK;
			return *ret_status;
		}
		else
		{
			if (current_col->next_col == NULL) {
				printf("out of columns to fill, dude\n");
				break;
			}
			current_col = current_col->next_col;
			printf("moving to next col on the list...\n");

		}
	}
}

void declare_handle(char* name, int_list* result, Var *var_pool) {

	Var* start = var_pool;

	// traverse to end of var pool, make sure this var name hasn't been declared before.
	while (start) {
		if (strcmp(name, start->var_name) == 0) {
			printf("Var has already been taken - choose a different var\n");
			return NULL;
		}
		else
			start = start->next;

	// create new Var object, populate with result.
	Var* new_var = malloc(sizeof(Var));
	strcpy(new_var->var_name, name);
	new_var->var_store = result;
	new_var->next = NULL;

	// link up pre-existing list to new var object.
	start->next = new_var;

	}
}

int_list* interpret_col_or_var (char* param1, Var* var_pool, Db* db_head) {
	char* period_pointer = strchr(param1, '.');
	if (period_pointer == NULL) {
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
		period_pointer = '\0';
		char* db_name = param1;
		char* tbl_name = period_pointer + 1;
		period_pointer = strchr(tbl_name, '.');
		if (period_pointer == NULL) {
			printf("insufficiently specified db - tbl - col set\n");
			return NULL;
		};
		period_pointer = '\0';
		char* col_name = period_pointer + 1;
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
}

int find_max(char* param1, char* param2, Db* db_head, Var* var_pool) {

int max = -2147483647;
int_list* param1_list;
int_list* param2_list;

param1_list = interpret_col_or_var(param1, var_pool, db_head);
if (param2)
	param2_list = interpret_col_or_var(param2, var_pool, db_head);

if (param1_list == NULL && param2_list == NULL) {
	printf("Neither input list has been found\n");
	return NULL;
}
else if (param1_list && !param2_list) {
	printf("Searching for simple max of param1 list\n");
	int_list* start = param1_list;
	while (start) {
		if (start->item > max)
			max = start->item;
		start = start->next;
	};
	return max;
}
else if (!param1_list && param2_list) {
	printf("searching for the position in param2 list that is the max\n");
	int pos_of_max = -1;
	int pos_in_file = -1;
	int_list* start = param2_list;
	while (start) {
		pos_in_file++;
		if (start->item > max) {
			max = start->item;
			pos_of_max = pos_in_file;
		};
		start = start->next;
	};
	return pos_of_max;
}
else {
	printf("searching for max of param1 list from positions in param 2 list\n");
	int_list* start1 = param1_list;
	int_list* start2 = param2_list;
	int pos_in_param1_list = 0;
	int pos_in_param2_list = 0;
	while(start2) {
		for (int x = pos_in_param1_list; x < start2->item; x++)
			start1 = start1->next;
		if (start1->item > max)
			max = start1->item;
		start2 = start2->next;
	};
	return max;
};
}

int find_min(char* param1, char* param2, Db* db_head, Var* var_pool) {

int min =  2147483647;

int_list* param1_list = interpret_col_or_var(param1, var_pool, db_head);
int_list* param2_list = NULL;
if (param2 != 0)
{
	int_list* param2_list = interpret_col_or_var(param2, var_pool, db_head);
}

if (!param1_list && !param2_list) {
	printf("Neither input list has been found\n");
	return NULL;
}
else if (param1_list && !param2_list) {
	printf("Searching for simple min of param1 list\n");
	int_list* start = param1_list;
	while (start) {
		if (start->item < min)
			min = start->item;
		start = start->next;
	};
	return min;
}
else if (!param1_list && param2_list) {
	printf("searching for the position in param2 list that is the min\n");
	int pos_of_min = -1;
	int pos_in_file = -1;
	int_list* start = param2_list;
	while (start) {
		pos_in_file++;
		if (start->item < min) {
			min = start->item;
			pos_of_min = pos_in_file;
		};
		start = start->next;
	};
	return pos_of_min;
}
else {
	printf("searching for max of param1 list from positions in param 2 list\n");
	int_list* start1 = param1_list;
	int_list* start2 = param2_list;
	int pos_in_param1_list = 0;
	int pos_in_param2_list = 0;
	while(start2) {
		for (int x = pos_in_param1_list; x < start2->item; x++)
			start1 = start1->next;
		if (start1->item > min)
			min = start1->item;
		start2 = start2->next;
	};
	return min;
};
}

void print_var(Var *var_pool, const char* create_arguments) {

    message_status mes_status;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));

    char *token;
    strcpy(tokenizer_copy, create_arguments);
    printf("%s\n", tokenizer_copy);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT)
            printf("syntax error\n");
        else {
	    // read and chop off last char, which should be a ')'
    		int last_char = strlen(token) - 1;
    		if (token[last_char] != ')')
	       		printf("syntax error\n");
	       	else
	       		token[last_char] = '\0';
	       	printf("variable name = %s\n", token);
    	}
    }
    // replace the ')' with a null terminating character. 
	    

		Var* start = var_pool;

		printf("looking for var name %s\n", token);
		if (start == NULL)
			printf("var pool is empty - nothing to print\n");
		else do
		{
			printf("Var pool member %s found\n", start->var_name);
			if (strcmp(start->var_name, token) == 0) {
				printf("setting up int_list to point to the var_store\n");
				int_list* results = start->var_store;
				if (results != NULL) {
					do
					{
						printf("%i ", results->item);
						if (results->next != NULL)
							results = results->next;
					} while (results != NULL && (results->next) != NULL);
					printf("\n");
					break;
				}
			}
			else
				start = start->next;
		} while (start != NULL);
}

int_list* select_row(const char* db_name, const char* table_name, const char* column_name, int low, int high, Db* head_db, const char* var_name, Var* var_pool) {

Status status;

// find the db
Db* target_db = lookup_db(db_name, head_db);
int found_var_name = 0;

// then find the table
Table* target_table = lookup_table(table_name, target_db);
if (strcmp(target_table->name, table_name) != 0)
{
	printf("Table doesn't match - can't execute select\n");
	status.code = ERROR;
	return NULL;
};
// then find the column
printf("looking for column %s in table %s\n", column_name, target_table->name);
Column* target_column = lookup_column(column_name, target_table);
if (target_column == NULL)
{
	printf("Couldn't find the specified column in the db/table combo. Can't execute select.\n");
	status.code = ERROR;
	return NULL;
};
// then initialize the var
Var* start = var_pool;
if (start->var_name[0] == NULL)
{
	printf("completely empty var pool right now... creating a new one.\n");
	//var_pool = malloc(sizeof(Var));
	strcpy(start->var_name, var_name);
	printf("copied %s into it\n", start->var_name);
	start->var_store = NULL;
	start->next = NULL;
}
else do
{
	printf("looking for var name %s, found %s in list\n", var_name, start->var_name);
	if (strcmp(start->var_name, var_name) == 0) {
		printf("found var name match in var pool list\n");
		found_var_name = 1;
		break;
	}
	else if (start->next == NULL) {
		printf("didn't find it and out of vars\n");
		Var* new_var = malloc(sizeof(Var));
		strcpy(new_var->var_name, var_name);
		new_var->var_store = NULL;
		new_var->next = NULL;
		start->next = new_var;
		break;
	} 
	else start = start->next;
}
while (start->var_name[0] != '\0');

//Var* var_pool_new = malloc(sizeof(Var));
//int_list* select_list_new = malloc(sizeof(int_list));
int_list* select_list_start = malloc(sizeof(int_list));
int_list* select_list = select_list_start;

select_list->item = NULL;	// assume it has nothing in the list unless populated otherwise.
start->var_store = select_list;
//if (!found_var_name)
//{
//	printf("couldn't find var name %s in var pool.", var_name);
//	strcpy(var_pool_new->var_name, var_name);
//	var_pool_new->next = NULL;
//};
int tuple_number = 0;
//int_list* select_list_current = select_list_new;
int_list* select_list_new = malloc(sizeof(int_list));;
printf("list of selected items: ");
Column* target_column_reserve = target_column;
int_list* target_column_reserve_data = target_column->data;
do
{
	printf("add %i? >%i and <%i?", target_column_reserve->data->item, low, high);
	if (target_column_reserve->data->item >= low && target_column_reserve->data->item <= high) 
	{
		printf(" yes\n");
		select_list->item = tuple_number;
		printf("%i ", select_list->item);
		select_list_new->item = NULL;
		select_list_new->next = NULL;
		select_list->next = select_list_new;
		select_list = select_list_new;
	}
	else printf("no\n");
	tuple_number++;
	target_column_reserve->data = target_column_reserve->data->next;
	select_list_new = malloc(sizeof(int_list));
	//int_list* select_list_new_2 = malloc(sizeof(int_list));
	//select_list_new->next = select_list_new_2;
	//select_list_new = select_list_new_2;
}
while (target_column->data->next != NULL);
printf("\n");

return select_list;

}


Status create_table(Db* db_current, char* name, int num_columns, Status *ret_status) {
	// If no table exists, create one
	if (db_current->tables == NULL) {				
		printf("table list is empty - creating a new one...\n");
		printf("table name %s\n", name);
		struct Table *new_table = (struct Table*) malloc(sizeof(struct Table));
		strcpy(new_table->name, name);	// copy the name
		new_table->col_count = num_columns;
		new_table->next_tbl = NULL;
		new_table->table_length = 0;
		db_current->tables = new_table;
		Column* current_col = malloc(sizeof(Column));
		Column* start_col = current_col;
		printf("Addr of start col = %i\n", start_col);
		new_table->columns = current_col;
		int_list* current_col_data = malloc(sizeof(int_list));
		current_col_data = NULL; // keep?
		new_table->columns->data = current_col_data;
		for (int x = 1; x < num_columns; x++) {
			// create a new column node
			Column* new_column = malloc(sizeof(Column));
			// create and initialize a new col
			int_list* new_col_data = malloc(sizeof(int_list));
			new_col_data = NULL;
			// hook column to new int list
			new_column->data = new_col_data;

			new_column->name[0] = '\0';

			current_col->next_col = new_column;
			printf("curr-col-next points to --> %i\n", current_col->next_col);
			current_col = new_column;
			printf("added col %i ...", x);
			printf("addr of latest column added is %i\n", current_col);
		}
		new_table->columns = start_col;
		printf("addr of START column is %i\n", new_table->columns);
	}
// NOTE: have to amend non-empty table multiplc column construction logic as well. TBD.
	else {
		printf("tbl list is not empty - having to traverse...\n");
		struct Table *temp_tbl = lookup_table(name, db_current);		
		if (temp_tbl->next_tbl == NULL) {									// In this case, the wasn't found. So create it...
			printf("couldn't find decalred name in the existing list, so creating new with %i columns...\n", num_columns);
			struct Table *new_table = (struct Table*) malloc(sizeof(struct Table));		// Go ahead and make it
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
			for (int x = 0; x <= num_columns; x++) {
				Column* new_column = malloc(sizeof(Column));
				//current_col->name = malloc(sizeof(char) * 64);
				current_col->data = NULL;
				current_col->name[0] = '\0';
				current_col->next_col = new_column;
				current_col = new_column;
			}
		}
	}
	ret_status->code = OK;
	return *ret_status;
}
/*	// Error checking: check to make sure table name doesn't already exist associated with this db

	// Creat a table object

	struct Table *table = (struct Table*) malloc(sizeof(Table));

	// DK: copy name of db into table structure, followed by a null terminator

	strcpy(table->name, name);
	table->columns = NULL;
	table->col_count = num_columns;
	table->table_length = 0;
	table->next_tbl = NULL;

	// DK: copy name of table into table structure, followed by a null terminator

	if (num_columns > 0) {
		struct Column* new_col = (struct Column*) malloc(sizeof(Column));
		table->columns = new_col;
		struct Column* next = NULL;
		for (unsigned int x = 0; x < num_columns; x++) {
			struct Column* new_col = (struct Column*) malloc(sizeof(Column));
			next->next_col = new_col;
		next->next_col = NULL;
	}


	}
	// DK: create a series of empty columns (to be named later)
	// The first column is pointed at by the table struct and there is a next pointer that can then point to another pointer to a
	// column pointer until ultimately it ends in NULL, which is the end of the list of columns.

	ret_status->code=OK;
	return NULL;
}
*/


/* 
 * Similarly, this method is meant to create a database.
 */
Db* create_db(const char* db_name, Db* db_head, Var* var_pool) {
	(void) (db_name);
	struct Status ret_status;
	struct Db* db_end;
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
	//var_pool->var_name = NULL;
	//var_pool->var_store = NULL;
	//var_pool->next = NULL;
	//ret_status.code = OK;
	}
}
// Checked over - this function looks ok
Status name_column(struct Column *column, const char* col_name, Status *ret_status) {
	Status localStatus = *ret_status;
	strcpy(col_name, column->name);
	localStatus.code = OK;
	return localStatus;
}

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

	while (temp_col->data != NULL)
		temp_col->data = temp_col->data->next;

	struct int_list *y;

	while (list != NULL) {
		y = malloc(sizeof(int_list));
		y->item = list->item;
		y->deleted_flag = 0;
		y->next = NULL;
		temp_col->data->next = y;
		list = list->next;
	};

ret_status = OK;
return *ret_status;
}

Status delete_row(const char* db_name, struct Db* db_head, const char* table_name, int column_number, int deletion_row, 
	struct Status *ret_status) {

// Find the database, return error if not found
// Find the table, return error if not found

	struct Db* temp_db = lookup_db(db_name, db_head);				// Have to define db_head -- isn't quite ther
	struct Table *temp_table = lookup_table(table_name, temp_db);
	struct Column *temp_col = temp_table->columns;

// Loop through each column in the table
//		Find the deletion_row row item (rows are counted if not already deleted), if it exists
//		Set deleted flag to TRUE

	struct int_list *start;
	start = malloc(sizeof(int_list));
	while (temp_col != NULL) {	
		start = temp_col->data;
		for (int y = 0; y < deletion_row && start != NULL; y+=(start->deleted_flag == 0))
			start = start->next;
		start->deleted_flag = 1;
		temp_col = temp_col->next_col;
	}
}