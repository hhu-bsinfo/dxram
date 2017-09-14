/*
 *	simple verb functionsm
 *
 *	24.6.2016, Michael Schoettner
 *	(modified version based on Master thesis of Michael Schlapa, HHU, 2016)
 */
#ifndef VERBS_H_SEEN
#define VERBS_H_SEEN

#include <infiniband/verbs.h>
#include <sys/types.h>


/* information about an ib connection */
typedef struct {
    int                 lid;			/* local ID assigned for port by subnet manager */
    int                 qpn;			/* physical QP number                   	*/
    int                 psn;			/* packet sequence number, never used 		*/
    unsigned            rkey;			/* remote key, needed to access mem reg 	*/
    unsigned long long  vaddr;			/* memory address, peer can later write to	*/
} ib_conn;


/* global information needed in several places */
typedef struct {
	struct ibv_device       **dev_list;     /* network device list          		*/
    struct ibv_context      *ib_ctx;        /* IB context                   		*/
    struct ibv_pd           *prot_dom;      /* protection domain            		*/
    struct ibv_mr           *mem_reg;       /* memory region                		*/
    struct ibv_comp_channel *comp_ch;       /* completion channel           		*/
    struct ibv_cq           *rcq;           /* receive completion queue     		*/
    struct ibv_cq           *scq;           /* send completion queue        		*/
    struct ibv_qp           *qp;            /* queue pair                   		*/

	pid_t 			pid;			/* my pid 					*/

	ib_conn 	 	local_conn;		/* information about local connection		*/
	ib_conn	 		*remote_conn;	/* info. about remote connection		*/

    void                    *data_buf; 	/* pointer to data buffer       		*/
	unsigned              	size;		/* size of data buf 				*/
	struct ibv_sge          sge_list;	/* scatter/gather list 				*/
	struct ibv_send_wr      wr;			/* work request for send queue of the qp 	*/
	struct ibv_recv_wr 		wr_recv;	/* work requests for receive queue of the qp */

} my_ib_context;


/* functions */
my_ib_context* create_ibv_ctx(int buf_size);
void destroy_ibv_ctx(my_ib_context *my_ctx);
void qp_change_state_init(struct ibv_qp *qp);
void get_local_ib_conn_info(my_ib_context *my_ctx);
void print_ib_connection_info(char *conn_name, ib_conn *conn);
int qp_change_state_rtr(my_ib_context *my_ctx);
int qp_change_state_rts(my_ib_context *my_ctx);
int message_send(my_ib_context *my_ctx);
int message_receive(my_ib_context *my_ctx);
int poll_completion(my_ib_context *my_ctx, struct ibv_cq* cq);
int rdma_write(my_ib_context *my_ctx);

#endif
