#define TRUE 1
#define FALSE 0

#include "client_context.h"
#include <string.h>
#include "utils.h"

/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 * 
 */
Db* lookup_db(char *name, struct Db *head_db) {

// Find the database
	Db* temp = head_db;					// current_db has to be the head of the list of databases.
	Db* result = malloc(sizeof(Db*));	// allocate a result db pointer
	char* db_name = malloc(100);		// allocate 100 bytes for a db_name
	*result->name = db_name;
	result = NULL;
	int a = 1;
	while (a != 0) {
		a = strcmp(temp->name, name);
		if (a == 0) 
			result = temp;
		if (temp->next_db == 0)
			break;
		else
			temp = temp->next_db;
	}; 

// Return error if not found
	if (a != 0) {
		free (result);
		return NULL;
	}
	else {
		strcpy(db_name, name);
		return result;
	}
}

Table* lookup_table(char *name, struct Db *db) {

// Find the table in the indicated db

	// If no table exists in the present db, initialize one with the provided name and return.
	if(db->tables == NULL) {
		Table* new_table = malloc(sizeof(Table));
		strcpy(new_table->name, name);
		new_table->columns = NULL;
		db->tables = new_table;
		return new_table;
	};

	// If there are tables, begin a search starting with the first table in the db
	Table* temp = db->tables;

	while (temp != NULL)
	{
		// compare search table with table name in temp
		// if it matches, the table sought has been found.
		if (strcmp(temp->name, name) == 0)
			return temp;
		temp = temp->next_tbl;
	};
	return NULL;
}

Column* lookup_column(char *name, struct Table* table) {

	// Locate the first column in a given table
	Column* temp = table->columns;
	Column* prev;

	name = trim_whitespace(name);
	while (temp) {
		if (strcmp(temp->name, name) == 0)
			return temp;
		prev = temp;
		temp = temp->next_col;
	};
	// if exiting the while loop, the column must not have been found, so return null.

	Column* new_column = malloc(sizeof(Column));
	strcpy(new_column->name, name);
	prev->next_col = new_column;
	int_list* new_list = malloc(sizeof(int_list));
	new_column->data = new_list;

	return new_column;
}

