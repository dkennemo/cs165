#include "client_context.h"
#include <string.h>
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 * 
 */
Db* lookup_db(char *name, struct Db *head_db) {

// Find the database


	Db* temp = head_db;		// current_db has to be the head of the list of databases. Not the case yet - revise definition above.
	Db* result = malloc(sizeof(Db*));
	char* db_name = malloc(64);
	*(result->name) = db_name;
	result = NULL;
	int a = 1;
	while (a != 0)
//	while (a != 0 && temp->empty_flag != 1)
	{
		printf("comparing %s and %s\n", temp->name, name);
		fflush(stdout);
		a = strcmp(temp->name, name);
		if (a == 0) 
			result = temp;
		else
			printf("this doesn't match...\n");
		if (temp->next_db == 0)
			break;
		else
		{
			printf("headed to next one, %s\n", temp->next_db->name);
			temp = temp->next_db;
		};
	}; 

// Return error if not found

	if (a != 0) {
		printf("Could not find database after checking them all\n");
		//free (temp);
		free (result);
		return NULL;
	}
	else {
		printf("Found database %s!\n", result->name);
		// free (temp);
		strcpy(db_name, name);
		return result;
	}
}

Table* lookup_table(char *name, struct Db *db) {

// Find the table in the indicated db

	printf("looking in tables for table %s\n", name);
	if(db->tables == NULL) {
		printf("there are no tables in this db yet\n");
		return NULL;
	};
	Table* temp = malloc(sizeof(Table));
	temp = db->tables;
	Table* result = NULL;

	int a = 1;
	while (a != 0 && temp->empty_flag != 1)
	{
		printf("comparing %s and %s\n", temp->name, name);
		fflush(stdout);
		a = strcmp(temp->name, name);
		if (a == 0)
			result = temp;
		else
			printf("this doesn't match...\n");
		if (temp->next_tbl == 0)
			break;
		else
		{
			printf("headed to next one, %i\n", temp->next_tbl)
			;
			temp = temp->next_tbl;
		};
	};

	if (a != 0) {
		printf("Could not find tables in db after checking them all\n");
		fflush(stdout);
		free (result);
		return temp;
	}
	else {
		printf("Found table %s!\n", result->name);
		fflush(stdout);
		//free (temp);
		return result;
	}
}

Column* lookup_column(char *name, struct Table* table) {

// Find the table in the indicated db

	Column* temp = table->columns;
	printf("lookup_column received parameters name = %s and table name = %s\n", name, table->name);
	int a = 1;
	printf("comparing column names %s and %s\n", temp->name, name);
	printf("%i %i\n", strlen(temp->name), strlen(name));
	name = trim_whitespace(name);
	a = strcmp(temp->name, name);
	printf("%i\n",a);
	while (a != 0 && temp->next_col != NULL) {
		printf("didn't fine a match and there's another col to search, so traversing...\n");
		temp = temp->next_col;
		a = strcmp(temp->name, name);
	};
// Return error if not found

	if (a != 0) {
		printf("Could not find column in table after checking them all\n");
		return NULL;
	}
	else
	{
		printf("Found the column - here's it's contents:\n");
		int_list* start = temp->data;
		printf("%s column name and %i is int_list start\n", temp->name, start);
		if (temp->data == NULL)
		{
			printf("col is empty - nothing to display\n");
		}
		else
		{
			do {
				printf("%i ",start->item);
				start = start->next;
			}
			while (start != NULL);
			printf("\nEnd of the list!\n");
		}
		return temp;
	}
}