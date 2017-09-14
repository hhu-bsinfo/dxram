/*
 *	simple tcp functions
 *	(need to exchange IB informations)
 *
 *      24.6.2016, Michael Schoettner
 *      (modified version based on Master thesis of Michael Schlapa, HHU, 2016)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <unistd.h>
#include "tcp.h"
#include "helper.h"


/*
 *  tcp_client_connect
 *  ------------------
 *  	Creates a connection to a TCP server
 */
int tcp_client_connect(my_ib_context *my_ctx, char *srv_name) {
    	char *service;                      	/* port number of server as string 	*/
    	int sockfd = -1;                    	/* socket file descriptor 		*/
        struct addrinfo *res;                   /* return of getaddrinfo                */
    	struct addrinfo hints = {
        	.ai_family      = AF_INET, 	/* IP4 or IP6 both ok? 			*/
        	.ai_socktype    = SOCK_STREAM	/* TCP 					*/
    	};


	/* convert port number to string */
   	asprintf(&service, "%d", TCP_PORT);

	/* get address for specified server */
    	TEST_N(getaddrinfo(srv_name, service, &hints, &res),
		"Getaddrinfo failed");

	/* create a socket for the client */
    	TEST_N(sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol),
        	"Could not create client socket");

        /* try to connect to first returned address record */
        TEST_N(connect(sockfd, res->ai_addr, res->ai_addrlen),
        	"Could not connect to server");

    	freeaddrinfo(res);

    	return sockfd;
}


/*
 *  tcp_server_listen
 *  -----------------
 *  	Creates a TCP server socket, which listens for incoming connections
 */
int tcp_server_listen(my_ib_context *my_ctx) {
        char *service;                          /* port number of server as string 	*/
        int sockfd = -1;                        /* socket file descriptor 		*/
        int connfd;                             /* returned socket for connection       */
   	struct addrinfo *res;			/* return of getaddrinfo 		*/
    	struct addrinfo hints = {
        	.ai_flags       = AI_PASSIVE,	/* needed for calling accept later 	*/
                .ai_family      = AF_UNSPEC,    /* IP4 or IP6 both ok?  		*/
                .ai_socktype    = SOCK_STREAM   /* TCP                  		*/
    	};


       	/* convert port number to string */
       	asprintf(&service, "%d", TCP_PORT);

	/* get address, note: server name is NULL, required for later calling accept  	*/
        TEST_N(getaddrinfo(NULL, service, &hints, &res),
		"Getaddrinfo failed");

	/* create a socket */
    	TEST_N(sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol),
        	"Could not create server socket");

	/* REUSE port even when busy */
    	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, NULL, 0);


	/* bind socket to port */
    	TEST_N(bind(sockfd, res->ai_addr, res->ai_addrlen),
        	"Could not bind addr to socket");

	/* wait for incoming connections */
    	listen(sockfd, 1);

	/* block here until a client connects */
    	TEST_N(connfd = accept(sockfd, NULL, 0),
        	"Server accept failed");

    	freeaddrinfo(res);

    	return connfd;
}


/*
 *  tcp_exch_ib_connection_info
 *  ---------------------------
 *    	Exchange IB verb info via tcp (both directions)r
 */
int tcp_exch_ib_connection_info(my_ib_context *my_ctx, int sockfd) {
    	char msg[sizeof "0000:000000:000000:00000000:0000000000000000"];
	ib_conn	*local  = &(my_ctx->local_conn);/* shortcut to local ib info 			*/
	ib_conn *remote = NULL;			/* shortcut to remote ib info, init. below 	*/
	int parsed;				/* number of parsed received data pieces 	*/

	/* fill-in info to be send */
    	sprintf(msg, "%04x:%06x:%06x:%08x:%016Lx",
        	local->lid, local->qpn, local->psn, local->rkey, local->vaddr);

	/* send my ib info to other peer */
    	if (write(sockfd, msg, sizeof msg) != sizeof msg) {
        	fprintf(stderr, "Error: Could not send connection_details to peer\n");
        	return -1;
    	}

	/* get remote ib info from other peer */
    	if (read(sockfd, msg, sizeof msg) != sizeof msg) {
        	fprintf(stderr, "Error: Could not receive connection_details to peer\n");
        	return -1;
    	}

	/* alloc memory for remote connection info */
    	TEST_Z(my_ctx->remote_conn = malloc(sizeof(ib_conn)),
        	"Could not allocate memory for remote_connection connection");
	remote = my_ctx->remote_conn;

	/* parse received data and get into remote_conn referenced by my_ctx */
   	parsed = sscanf(msg, "%x:%x:%x:%x:%Lx",
        		&remote->lid, &remote->qpn, &remote->psn, &remote->rkey, &remote->vaddr );
    	if (parsed != 5) {
        	fprintf(stderr, "Error: Could not parse message from peer\n");
        	free(my_ctx->remote_conn);
    	}
    	return 0;
}

