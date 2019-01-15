/** server.c
 * CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 * 
 * Getting started hints: 
 *      What are the structural attributes of a `query`?
 *         A query has various operands that tell the db what it's looking for and where. It is the db's job to find it and
 *         then perform whatever action is needed.
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/

/*
char* execute_DbOperator(char query[100], int client_socket, struct Db* db_head, Var* var_pool) {
 
    struct message* message = malloc(sizeof(message));                                          // for the server to return, not supplied to it.
    struct DbOperator *dbreturn = malloc(sizeof(DbOperator));
    struct ClientContext* cc = malloc(sizeof(ClientContext));   // for the server to return, not supplied to it.

    printf("Going into parse command...\n");
    printf("query = %s\n", query);
    printf("message = %s\n", message->payload);
    printf("client socket = %i\n", client_socket);
    printf("length of query = %li\n", strlen(query));
    fflush(stdout);
    dbreturn = parse_command(query, message, client_socket, cc, db_head, var_pool, batch_mode);
    printf("Back in execute_Dboperator...\n");
    free(query);
    free(message);
    free(cc);

    printf("returning to handle client\n");
    return dbreturn;
}
*/

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket, Db* db_head) {
    int done = 0;
    int length = 0;
    int wait = 0;

    log_info("Connected to socket: %d.\n", client_socket);
    Var* var_pool = malloc(sizeof(Var));
    var_pool->next = malloc(sizeof(Var*));
    var_pool->next = NULL;
    var_pool->var_name[0] = '\0';
    var_pool->var_store = malloc(sizeof(int_list*));
    var_pool->var_store = NULL;

    int* batch_mode = malloc(sizeof(int));                     // initializes not in batch mode
    Batch_list* batch = malloc(sizeof(Batch_list*));

    *batch_mode = 0;
    batch = NULL;

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = NULL;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        // Server is not supposed to shut down just because client does. 
        if (length < 0) {
            wait = 1;
        } else if (length == 0) {
            done = 1;
        } else if (length == 1) {
            wait = 1;
        } else wait = 0;
        printf("Done = %i, Wait = %i, length = %i\n", done, wait, length);

        if (!done && !wait) {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            //    Query string is converted into a request for an database operator
            parse_command(recv_message.payload, &send_message, client_socket, client_context, db_head, var_pool, batch_mode, batch);

            // 2. Handle request
            //    Corresponding database operator is executed over the query
            // DbOperator* intermediate = execute_DbOperator(query, client_socket);
            char* result = malloc(sizeof(char) * 100);
            strcpy(result, recv_message.payload);

            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;
            
            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response to the request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
// 
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients? 
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)
int main(void)
{
    struct Db* db_head = malloc(sizeof(Db));
    db_head->empty_flag = 1;
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    while (1) {

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket, db_head);

    };
    //return 0;
}
