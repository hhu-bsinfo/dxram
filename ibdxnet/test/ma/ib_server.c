/*
 *	simple IB RDMA write test server program
 *
 *	24.6.2016, Michael Schoettner
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


static void rdma(my_ib_context* my_ctx, int op_count, int buf_size) {
	test_data 		*times;			/* data for RDMA writes 		*/
	int 			i;				/* counter	 			*/
	char 			*chPtr;			/* pointer to data buffer for TCP 	*/
	float 			avg_lat=0.0;	/* average latency for rdma write 	*/

	printf("Doing RDMA-WRITE ...\n");

   	times = malloc(op_count * sizeof(test_data));

   	for (i = 0; i < op_count; i++) {
   		chPtr = my_ctx->data_buf;
   		sprintf(chPtr, "%d", i);
  		gettimeofday(&times[i].tposted, NULL);
   		rdma_write(my_ctx);
		poll_completion(my_ctx, my_ctx->scq);
   		gettimeofday(&times[i].tcompleted, NULL);
   	}

   	for (i = 0; i < op_count; i++) {
   		avg_lat += calc_time_delta(times[i].tposted, times[i].tcompleted);
  	}
	int totalTime = avg_lat;
	avg_lat /= op_count;
	printf("\n *** %d RDMA_WRITES resulted in an average latency of %.02fus ***\n\n",
	op_count, avg_lat);
	printf("%f MB/sec\n", (double) op_count * buf_size / 1024.0 / 1024.0 / totalTime * 1000 * 1000);

  	free(times);
}

static void message(my_ib_context* my_ctx, int op_count, int buf_size) {
	test_data 		*times;			/* data for RDMA writes 		*/
	int 			i = 0;				/* counter	 			*/
	char 			*chPtr;			/* pointer to data buffer for TCP 	*/
	float 			avg_lat=0.0;	/* average latency for rdma write 	*/

	printf("Doing MESSAGE_SEND ...\n");

   	times = malloc(op_count * sizeof(test_data));

   	for (i = 0; i < op_count / 100; i++) {
   		chPtr = my_ctx->data_buf;
   		sprintf(chPtr, "%d", i);
  		gettimeofday(&times[i].tposted, NULL);
		for (int i = 0; i < 100; i++) {
   			message_send(my_ctx);
		}
		for (int i = 0; i < 100; i++) {
			poll_completion(my_ctx, my_ctx->scq);
		}
   		gettimeofday(&times[i].tcompleted, NULL);
   	}

   	for (i = 0; i < op_count; i++) {
   		avg_lat += calc_time_delta(times[i].tposted, times[i].tcompleted);
  	}
	int totalTime = avg_lat;

	avg_lat /= op_count;
	printf("\n *** %d MESSAGE_SEND resulted in an average latency of %.02fus ***\n\n",
	op_count, avg_lat);
	printf("%f MB/sec\n", (double) op_count * buf_size / 1024.0 / 1024.0 / totalTime * 1000 * 1000);

  	free(times);	
}


/*
 * main
 * ----
 */
int main(int argc, char *argv[]) {
 	my_ib_context 	*my_ctx=NULL;	/* globals for ib stuff 		*/
	int 			sockfd;			/* tcp socket file descriptor 		*/
	int 			mode = 0;		/* 0 = rdma mode, 1 = message mode */

	if (argc < 3) {
		printf("Usage: %s <buf_size> <op_count> [mode: rdma, msg]\n", argv[0]);
		return -1;
	}

	if (argc > 3) {
		if (!strcmp(argv[3], "rdma")) {
			mode = 0;
		} else if (!strcmp(argv[3], "msg")) {
			mode = 1;
		} 
	}

	int buf_size = atoi(argv[1]);
	int op_count = atoi(argv[2]);	
	printf("\nIB server, buf_size %d, op_count %d, mode: %s\n", buf_size, op_count, !mode ? "rdma" : "msg");
	printf("----------------------------------\n");

  	/*
     * Create and init IB queue pair
     */
	printf("Create and init IB queue pair.\n");
	my_ctx = create_ibv_ctx(buf_size);


	/*
	 * Waiting for client to connect via TCPr
	 */
    printf("Setting up TCP connection.\n");
	printf("   Waiting for client to connect.\n");
    TEST_N(sockfd = tcp_server_listen(my_ctx), "tcp_server_listen (TCP) failed");
	printf("   Client connected.\n");

    /*
     * Exchange IB connection information over TCP
     */
    printf("Exchanging IB information via TCP.\n");
	TEST_NZ(tcp_exch_ib_connection_info(my_ctx, sockfd),
    	"Could not exchange connection info via TCP");
    print_ib_connection_info("   Local IB connection ", &(my_ctx->local_conn) );
    print_ib_connection_info("   Remote IB connection", my_ctx->remote_conn );


   /*
    * Set queue pair to ready to send
    */
	printf("Setting IB to ready to send.\n");
	qp_change_state_rts(my_ctx);

	if (mode == 0) {
		rdma(my_ctx, op_count, buf_size);
	} else {
		message(my_ctx, op_count, buf_size);
	}

	printf("Done. Sending 'close' over TCP to client.\n");
   	if ( write(sockfd, "close", sizeof("close")) != sizeof("close")) {
   		fprintf(stderr, "Error: Could not tell client to stop\n");
   		return -1;
  	}

	/*
	 * We are done. Clean up.
	 */
	printf("Destroy IB data structures.\n");
	destroy_ibv_ctx(my_ctx);

   	printf("Closing TCP socket.\n");
	close(sockfd);

	return 0;
}
