/*
 *      helper macros to keep code compact
 *
 *      20.3.2016, Michael Schlapa
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include "helper.h"


/*
 * die
 * ---
 *	Quit program (called if an unrecoverable error occurred)
 */
int die(char *reason) {
    fprintf(stderr, "   Error: %s - %s\n ", strerror(errno), reason);
    exit(EXIT_FAILURE);
    return -1;
}


/*
 * calc_time_delta
 * ---------------
 *      Return time difference.
 */
uint64_t calc_time_delta(struct timeval start, struct timeval end) {
    struct timeval delta;
    timersub(&end, &start, &delta);
    return delta.tv_sec * (uint64_t)1000000 + delta.tv_usec;
}

