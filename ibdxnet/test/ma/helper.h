/*
 *	helper macros to keep code compact
 *
 *	20.3.2016, Michael Schlapa
 */
#include <time.h>
#include <stdint.h>


/* functions */
int die(char *reason);
uint64_t calc_time_delta(struct timeval start, struct timeval end);


/* some macros making source code more compact */
#define TEST_NZ(x,y) do { if ((x)) die(y); } while (0)
#define TEST_Z(x,y) do { if (!(x)) die(y); } while (0)
#define TEST_N(x,y) do { if ((x)<0) die(y); } while (0)
