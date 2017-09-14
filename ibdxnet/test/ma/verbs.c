/*
 *      simple verb functionsm
 *
 *      24.6.2016, Michael Schoettner
 *      (modified version based on Master thesis of Michael Schlapa, HHU, 2016)
 */

#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <malloc.h>
#include <infiniband/verbs.h>
#include <string.h>
#include <unistd.h>
#include "verbs.h"
#include "helper.h"


/* some constants */
#define SEND_WRID				0
#define RECV_WRID				1
#define RDMA_WRID               3               /* ID of RDMA write             	*/
#define DEF_TX_DEPTH 		100		/* max. outstanding send reqs.		*/
#define DEF_RX_DEPTH 		100		/* max. outstanding recv reqs.		*/
#define DEF_IB_PORT		1		/* default ib port 			*/
#define DEF_IB_SERVICE_LEVEL	1		/* default QoS priority			*/


/*
 * create_ibv_ctx
 * --------------
 *	1. get network device list
 *	2. open IB device
 *	3. alloc protection domain
 *	4. register memory region
 *	5. create completion channel
 *	6. create completion queues for sending & receiving
 *	7. creat queue pair
 *  @param buf_size Defines either buffer size for messages or memory region size for RDMA
 */
my_ib_context* create_ibv_ctx(int buf_size) {
	/* alloc our globals */
	my_ib_context *my_ctx = malloc(sizeof(my_ib_context));
    memset(my_ctx, 0, sizeof(my_ib_context));

	/* fill in default values */
	my_ctx->size     = buf_size;
	my_ctx->pid	 = getpid();

	/* get device */
	TEST_Z(	my_ctx->dev_list = ibv_get_device_list(NULL), "No IB-device available.\n" );

 	/* open device 0 */
    TEST_Z( my_ctx->ib_ctx = ibv_open_device(my_ctx->dev_list[0]), "Could not open IB dev.\n" );

	struct ibv_device_attr device_attr;
	TEST_NZ( ibv_query_device(my_ctx->ib_ctx, &device_attr), "Querying device info failed\n");

	printf("device max_mr_size: %d\n", device_attr.max_mr_size);

    /* alloc protection domain */
    TEST_Z( my_ctx->prot_dom = ibv_alloc_pd(my_ctx->ib_ctx), "Could not alloc protection domain.\n" );

    /* alloc buffer */
    TEST_Z( my_ctx->data_buf = memalign(sysconf(_SC_PAGESIZE), my_ctx->size), "Could not alloc data buffer.\n" );
    memset(my_ctx->data_buf, 0, my_ctx->size);

   	/* register memory region
 	*
	 * We dont really want IBV_ACCESS_LOCAL_WRITE, but IB spec says:
 	 * The Consumer is not allowed to assign Remote Write or Remote Atomic to
 	 * a Memory Region that has not been assigned Local Write.
 	 */
   	TEST_Z( my_ctx->mem_reg = ibv_reg_mr(my_ctx->prot_dom, my_ctx->data_buf, my_ctx->size,
                            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE ),
                            "Could not alloc memory region.\n" );

 	/* create send & receive completion queues and connect BOTH to the channel */
    TEST_Z( my_ctx->rcq = ibv_create_cq(my_ctx->ib_ctx, DEF_RX_DEPTH, my_ctx->ib_ctx, NULL, 0),
            "Could not create receive completion queue.\n" );
    TEST_Z( my_ctx->scq = ibv_create_cq(my_ctx->ib_ctx, DEF_TX_DEPTH, my_ctx->ib_ctx, NULL, 0),
            "Could not create send completion queue.\n" );

	/* create queue pair */
        struct ibv_qp_init_attr qp_init_attr = {
                                        .send_cq = my_ctx->scq,
                                        .recv_cq = my_ctx->rcq,
                                        .qp_type = IBV_QPT_RC,  	/* reliable connection 		*/
                                        .cap = {
                                                .max_send_wr     = DEF_TX_DEPTH,
                                                .max_recv_wr     = DEF_RX_DEPTH,   /* outstanding rcv req.         */
                                                .max_send_sge    = 1, 	/* #S/G entries per snd req     */
                                                .max_recv_sge    = 1,   /* #S/G entries per rcv req     */
                                                .max_inline_data = 0    /* req inline data in bytes     */
                                                }			/* S/G = Scatter / Gather       */
                                        };

       	TEST_Z( my_ctx->qp = ibv_create_qp(my_ctx->prot_dom, &qp_init_attr),
                "Queue pair creation failed.\n" );

	/* set queue pair status to INIT */
	qp_change_state_init(my_ctx->qp);

	/* Get local connection info -> stored in my_ctx variables.
	   Must be sent to other node via TCP later */
        get_local_ib_conn_info(my_ctx);

	return my_ctx;
}


/*
 *  qp_change_state_init
 *  --------------------
 *  	Changes Queue Pair status to INIT
 */
void qp_change_state_init(struct ibv_qp *qp) {
    	struct ibv_qp_attr *attr;

    	attr =  malloc(sizeof *attr);
    	memset(attr, 0, sizeof *attr);

    	attr->qp_state        = IBV_QPS_INIT;
    	attr->pkey_index      = 0;
    	attr->port_num        = DEF_IB_PORT;
    	attr->qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

   	/* modify queue pair attributes */
    	TEST_NZ(ibv_modify_qp(qp, attr,
        	IBV_QP_STATE        |
        	IBV_QP_PKEY_INDEX   |
        	IBV_QP_PORT         |
        	IBV_QP_ACCESS_FLAGS),
        	"Could not modify QP to INIT, ibv_modify_qp");
}


/*
 *  get_local_ib_conn_info
 *  ----------------------
 *  	Gets all relevant attributes needed for an IB connection.
 *  	Those are then sent later (not in this function) to the peer via TCP.
 *  	Information needed to exchange messages over IB are:
 *     	  lid - Local Identifier, 16 bit address assigned to end node by subnet manager
 *     	  qpn - Queue Pair Number, identifies qpn within channel adapter (HCA)
 *     	  psn - Packet Sequence Number, used to verify correct delivery sequence of packages (similar to ACK)
 *     	  rkey - Remote Key, together with 'vaddr' identifies and grants access to memory region
 *        vaddr - Virtual Address, memory address that peer can later write to
 */
void get_local_ib_conn_info(my_ib_context *my_ctx) {
    	struct ibv_port_attr attr;

	/* get local lid */
	TEST_NZ(ibv_query_port(my_ctx->ib_ctx, DEF_IB_PORT, &attr),
	        "Could not get port attributes, ibv_query_port");

    	my_ctx->local_conn.lid   = attr.lid;		/* local ID assigned by subnet mgr. 	*/
    	my_ctx->local_conn.qpn   = my_ctx->qp->qp_num;	/* physical QP number 			*/
    	my_ctx->local_conn.psn   = lrand48() & 0xffffff;/* packet sequence number, never used 	*/
    	my_ctx->local_conn.rkey  = my_ctx->mem_reg->rkey;/* remote key, needed to access mem reg*/
   	my_ctx->local_conn.vaddr = (uintptr_t)my_ctx->data_buf;/* memory addr. peer can later write to*/
}


/*
 * destroy_ibv_ctx
 * ---------------
 */
void destroy_ibv_ctx(my_ib_context *my_ctx) {
	ibv_destroy_qp(my_ctx->qp);			/* destroy queue pair 			*/
	ibv_destroy_cq(my_ctx->scq); 			/* destroy send completion queue 	*/
	ibv_destroy_cq(my_ctx->rcq);			/* destroy receive completion queue 	*/
	ibv_dereg_mr(my_ctx->mem_reg);			/* dregister memory region 		*/
 	ibv_dealloc_pd(my_ctx->prot_dom);		/* free protection domain 		*/
        ibv_close_device(my_ctx->ib_ctx);		/* close device 			*/

	free(my_ctx);
}


/*
 * qp_change_state_rtr
 * -------------------
 * 	Change queue pair state to ready to receive.
 */
int qp_change_state_rtr(my_ib_context *my_ctx) {
	struct ibv_qp_attr *attr;

	/* alloc & init attribute buffer */
    	attr =  malloc(sizeof *attr);
    	memset(attr, 0, sizeof *attr);

    	attr->qp_state              = IBV_QPS_RTR;		/* ready to receive state 		*/
    	attr->path_mtu              = IBV_MTU_2048;		/* MTU SIZE = 2048 bytes 		*/
    	attr->dest_qp_num           = my_ctx->remote_conn->qpn;	/* server qp_num 			*/
    	attr->rq_psn                = my_ctx->remote_conn->psn;	/* server packet seq. nr. 		*/
								/* nr of responder resources for	*/
        attr->max_dest_rd_atomic    = 1;			/* incomingg RDMA reads & atomic ops 	*/
    	attr->min_rnr_timer         = 12;			/* minimum RNR NAK timer 		*/
    	attr->ah_attr.is_global     = 0;			/* global routing header not used 	*/
    	attr->ah_attr.dlid          = my_ctx->remote_conn->lid;	/* LID of remote IB port		*/
    	attr->ah_attr.sl            = DEF_IB_SERVICE_LEVEL;	/* QoS priority 			*/
    	attr->ah_attr.src_path_bits = 0;			/* default port (for multiport NICs) 	*/
    	attr->ah_attr.port_num      = DEF_IB_PORT;		/* IB port 				*/

    	TEST_NZ(ibv_modify_qp(my_ctx->qp, attr,			/* do the state change on the qp 	*/
        	IBV_QP_STATE                |
        	IBV_QP_AV                   |
        	IBV_QP_PATH_MTU             |
        	IBV_QP_DEST_QPN             |
        	IBV_QP_RQ_PSN               |
       	 	IBV_QP_MAX_DEST_RD_ATOMIC   |
        	IBV_QP_MIN_RNR_TIMER),
        	"Could not modify QP to RTR state");

    	free(attr);

    	return 0;
}


/*
 * qp_change_state_rts
 * -------------------
 *      Change queue pair state to ready to send.
 */
int qp_change_state_rts(my_ib_context *my_ctx) {
       	struct ibv_qp_attr *attr;

        /* alloc & init attribute buffer */
        attr =  malloc(sizeof *attr);
        memset(attr, 0, sizeof *attr);

    	//* first the qp state has to be changed to rtr */
    	qp_change_state_rtr(my_ctx);

    	attr->qp_state        	= IBV_QPS_RTS;			/* ready to send state 		*/
    	attr->timeout           = 14;				/* local ack timeout 		*/
    	attr->retry_cnt         = 7;				/* retry count 			*/
    	attr->rnr_retry         = 7;    			/* rnr=receiver not ready 	*/
    	attr->sq_psn            = my_ctx->local_conn.psn;	/* packet sequence number 	*/
    	attr->max_rd_atomic     = 1;				/* nr of outstanding RDMA reads */
								/* & atomic ops on dest. qp 	*/

        TEST_NZ(ibv_modify_qp(my_ctx->qp, attr,                 /* do the state change on the qp*/
        	IBV_QP_STATE            |
        	IBV_QP_TIMEOUT          |
        	IBV_QP_RETRY_CNT        |
        	IBV_QP_RNR_RETRY        |
        	IBV_QP_SQ_PSN           |
        	IBV_QP_MAX_QP_RD_ATOMIC),
        	"Could not modify QP to RTS state");

    	free(attr);

    	return 0;
}


/*
 * print_ib_connection_info
 * -------------------------
 */
void print_ib_connection_info(char *conn_name, ib_conn *conn) {
	printf("%s: LID %#04x, QPN %#06x, PSN %#06x RKey %#08x VAddr %#016Lx\n",
        	conn_name, conn->lid, conn->qpn, conn->psn, conn->rkey, conn->vaddr);
}

/*
 * message_send
 * -----------
 * Send data from my_ctx->data_buf in a message passing manner to the remote
 * The remote is required to call message_receive to queue a receive work request
 * to allow the data to get received
 */
int message_send(my_ib_context *my_ctx) {
		struct 	ibv_send_wr *bad_wr;	/* first failed work request 	*/
	struct 	ibv_cq 		*ev_cq;		/* cq that got a completion 	*/
	void 				*ev_ctx;	/* context of ev_cq		*/

	/* hook our local buffer with the message contents */
	my_ctx->sge_list.addr      		= (uintptr_t)my_ctx->data_buf;	/* data source 			*/
	my_ctx->sge_list.length    		= my_ctx->size;					/* data source length 		*/
	my_ctx->sge_list.lkey      		= my_ctx->mem_reg->lkey;		/* local key of mem reg. 	*/

	/* create a work request for our send operation with one buffer element */
	my_ctx->wr.wr_id       			= SEND_WRID;					/* own work request ID		*/
	my_ctx->wr.sg_list     			= &my_ctx->sge_list;			/* scatter/gather array 	*/
	my_ctx->wr.num_sge     			= 1;							/* size of sg_list		*/
	my_ctx->wr.opcode      			= IBV_WR_SEND;					/* opcode for message send 	*/
	/* set complettion notification */
	my_ctx->wr.send_flags  			= IBV_SEND_SIGNALED;			/* for this work request 	*/
	my_ctx->wr.next        			= NULL;							/* last entry in work list 	*/

	/* posts a linked list of work requests (WRs) to the send queue of a qp */
	TEST_NZ(ibv_post_send(my_ctx->qp, &my_ctx->wr, &bad_wr), "ibv_post_send failed. This is bad mkey");

	return 0;
}

int poll_completion(my_ib_context *my_ctx, struct ibv_cq* cq)
{
	struct 	ibv_wc 		wc;			/* work complections  		*/
	int 				ne;			/* number/error work compl.	*/

	/* empty the CQ: poll all of the completions from the CQ (if any exist) */
	/* scq: poll send completion queue, get max. 1 entry, wc: array of work completions */
retry:	
	do {
		ne = ibv_poll_cq(cq, 1, &wc);		
	} while (ne == 0);

	if (ne < 0) {
		fprintf(stderr, "Error: failed to poll completions from the CQ: ret = %d\n", ne);
		return -1;
	}

	if (wc.status) {
		if (wc.status != IBV_WC_WR_FLUSH_ERR)
			fprintf(stderr, "cq completion failed status %d\n",
				wc.status);
		else
			goto retry;
		return -1;
	}
		
	/* one can further evaluate opcodes if the queue is used for multiple operation types */
	/* using wc.opcode */

	return 0;
}

/*
 * message_receive
 * -----------
 * Receive data from a remote instance and write it to my_ctx->data_buf in a message passing manner
 * The remote is required to call message_send to issue a send request with data
 * to allow the data to get received here
 */
int message_receive(my_ib_context *my_ctx) {
	struct 	ibv_recv_wr *bad_wr;	/* first failed work request 	*/
	struct 	ibv_cq 			*ev_cq;		/* cq that got a completion 	*/
	void 					*ev_ctx;	/* context of ev_cq		*/

	/* hook our local buffer, which receives the message contents */
	memset(&my_ctx->sge_list, 0, sizeof(my_ctx->sge_list));
	my_ctx->sge_list.addr      		= (uintptr_t)my_ctx->data_buf;	/* data source 			*/
	my_ctx->sge_list.length    		= my_ctx->size;					/* data source length 		*/
	my_ctx->sge_list.lkey      		= my_ctx->mem_reg->lkey;		/* local key of mem reg. 	*/

	memset(&my_ctx->wr_recv, 0, sizeof(my_ctx->wr_recv));
	my_ctx->wr_recv.wr_id       	= RECV_WRID;					/* own work request ID		*/
	my_ctx->wr_recv.sg_list     	= &my_ctx->sge_list;			/* scatter/gather array 	*/
	my_ctx->wr_recv.num_sge    		= 1;							/* size of sg_list		*/
	my_ctx->wr_recv.next        	= NULL;							/* last entry in work list 	*/	


	/* posts a linked list of work requests (WRs) to the receive queue of a qp */
	TEST_NZ(ibv_post_recv(my_ctx->qp, &my_ctx->wr_recv, &bad_wr), "ibv_post_recv failed. This is bad mkey");

	return 0;
}

/*
 * rdma_write
 * -----------
 * 	Write data in my_ctx->data_buf to remote buffer
 */
int rdma_write(my_ib_context *my_ctx) {
	struct 	ibv_send_wr *bad_wr;	/* first failed work request 	*/
    	struct 	ibv_cq 		*ev_cq;					/* cq that got a completion 	*/
    	void 	*ev_ctx;						/* context of ev_cq		*/

    	my_ctx->sge_list.addr      = (uintptr_t)my_ctx->data_buf;	/* data source 			*/
    	my_ctx->sge_list.length    = my_ctx->size;			/* data source length 		*/
    	my_ctx->sge_list.lkey      = my_ctx->mem_reg->lkey;		/* local key of mem reg. 	*/

    	my_ctx->wr.wr.rdma.remote_addr 	= my_ctx->remote_conn->vaddr;
    	my_ctx->wr.wr.rdma.rkey        	= my_ctx->remote_conn->rkey;
    	my_ctx->wr.wr_id       		= RDMA_WRID;			/* own work request ID		*/
    	my_ctx->wr.sg_list     		= &my_ctx->sge_list;		/* scatter/gather array 	*/
    	my_ctx->wr.num_sge     		= 1;				/* size of sg_list		*/
    	my_ctx->wr.opcode      		= IBV_WR_RDMA_WRITE;		/* opcode for rdma write 	*/
									/* set complettion notification */
    	my_ctx->wr.send_flags  		= IBV_SEND_SIGNALED;		/* for this work request 	*/
    	my_ctx->wr.next        		= NULL;				/* last entry in work list 	*/


	/* posts a linked list of work requests (WRs) to the send queue of a qp */
    	TEST_NZ(ibv_post_send(my_ctx->qp, &my_ctx->wr, &bad_wr),
        	"ibv_post_send failed. This is bad mkey");

	return 0;

}

