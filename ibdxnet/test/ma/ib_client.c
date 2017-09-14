/*
 *	simple IB RDMA WRITE test client program
 *
 *	26.6.2016, Michael Schoettner
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include "verbs.h"
#include "tcp.h"
#include "helper.h"


typedef struct {
    struct timeval	tposted;
    struct timeval   	tcompleted;
} test_data;

/*
 * main
 * ----
 */
int main(int argc, char *argv[]) {
 	my_ib_context *my_ctx=NULL;		/* globals for ib stuff 		*/
	int  sockfd;					/* tcp socket file descriptor 		*/
 	char *chPtr;					/* TCP data buffer 			*/
	int 			mode = 0;		/* 0 = rdma mode, 1 = message mode */

	/*
	 * parse arguments
	 */
	if (argc < 4) {
		printf("Usage: %s <host> <buf_size> <op_count> [mode]\n", argv[0]);
		return -1;
	}

	if (argc > 4) {
		if (!strcmp(argv[4], "rdma")) {
			mode = 0;
		} else if (!strcmp(argv[4], "msg")) {
			mode = 1;
		} 
	}

	int buf_size = atoi(argv[2]);
	int op_count = atoi(argv[3]);
	printf("\nIB client, buf_size %d, op_count %d, mode: %s", buf_size, op_count, !mode ? "rdma" : "msg");
	printf("----------------------------------\n");

  	/*
     * Create and init IB queue pair
     */
	printf("Create and init IB queue pair.\n");
	my_ctx = create_ibv_ctx(buf_size);


	/*
	 * Set up a TCP connection between client and server
	 */
    printf("Setting up TCP connection.\n");
    sockfd = tcp_client_connect(my_ctx, argv[1]);
	printf("   Connected to server.\n");


    /*
     * Exchange IB connection information over TCP
     */
    printf("Exchanging IB information via TCP.\n");
	TEST_NZ(tcp_exch_ib_connection_info(my_ctx, sockfd),
    	"Could not exchange connection info via TCP");
    print_ib_connection_info("   Local IB connection ", &(my_ctx->local_conn) );
    print_ib_connection_info("   Remote IB connection", my_ctx->remote_conn );


   /*
    * Set client queue to read to receive
    */
	printf("Setting IB to ready to receive.\n");
	qp_change_state_rtr(my_ctx);

	if (mode == 1) {
		for (int i = 0; i < op_count / 100; i++) {
			for (int i = 0; i < 100; i++) {
				message_receive(my_ctx);
			}

			for (int i = 0; i < 100; i++) {
				poll_completion(my_ctx, my_ctx->rcq);
			}
		}
	}

	/*
	 * Check if receive the close command from the server via TCP
	 */
	printf("Waiting for 'close' over TCP.\n");
   	chPtr = malloc(sizeof("close"));
   	if (read(sockfd, chPtr, sizeof("close")) != sizeof("close")) {
   		fprintf(stderr,"Error: wrong packet read!\n");
   	}
   	free(chPtr);
   	printf("Server sent 'close', going down. Closing TCP socket.\n");
  	close(sockfd);


	/*
	 * We are done. Clean up.
	 */
	printf("Destroy IB data structures.\n");
	destroy_ibv_ctx(my_ctx);

	printf("Results are printed on server.\n");

	return 0;
}
