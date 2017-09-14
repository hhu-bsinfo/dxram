/*
 *      simple tcp functions
 *      (need to exchange IB informations)
 *
 *      24.6.2016, Michael Schoettner
 *      (modified version based on Master thesis of Michael Schlapa, HHU, 2016)
 */

#include "verbs.h"


#define TCP_PORT	18515		/* port for tcp connection */


/* functions */
int tcp_client_connect(my_ib_context *my_ctx, char *srv_name);
int tcp_server_listen(my_ib_context *my_ctx);
int tcp_exch_ib_connection_info(my_ib_context *my_ctx, int sockfd);
