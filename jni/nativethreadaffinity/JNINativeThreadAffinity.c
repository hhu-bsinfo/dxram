/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

// compile:
// linux:
// gcc -O2 -shared -fpic -o libJNINativeThreadAffinity.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeThreadAffinity.c

#define _GNU_SOURCE
#include <jni.h>
#include <sys/sysinfo.h>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>


/**
 * Set CPU core affinity for calling thread to given core.
 *
 * @param p_core
 *         the core to pin to.
 * @return 0 if successful, != 0 otherwise
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIThreadAffinity_pinToCore(JNIEnv *p_env, jclass p_class, jint p_core) {
	int s, j;
	int number_of_cores;
	cpu_set_t cpuset;
	pthread_t thread;

	thread = pthread_self();

	// Get number of cores
	number_of_cores = get_nprocs_conf();
	// Clear cpu set
	CPU_ZERO(&cpuset);
	// Add core to cpu set
	CPU_SET(p_core, &cpuset);

	// Set affinity
	s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
	if (s != 0) {
		return s;
	}

	// Check affinity
	s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
	if (s != 0) {
	   return s;
	}
	
	return 0;
}

/**
 * Set CPU core affinity for calling thread to given core set.
 *
 * @param p_coreSet
 *         a set of cores to pin to.
 * @return 0 if successful, != 0 otherwise
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIThreadAffinity_pinToCoreSet(JNIEnv *p_env, jclass p_class, jintArray p_coreSet) {
	int s, j;
	int number_of_cores;
	cpu_set_t cpuset;
	pthread_t thread;
	
	jint *cores = (*p_env)->GetIntArrayElements(p_env, p_coreSet, NULL);
	if (cores == NULL) {
		return -2;
	}
	jsize length = (*p_env)->GetArrayLength(p_env, p_coreSet);


	thread = pthread_self();

	// Get number of cores
	number_of_cores = get_nprocs_conf();
	// Clear cpu set
	CPU_ZERO(&cpuset);
	// Add cores to cpu set
	for (int i = 0; i < length; i++) {
		CPU_SET(cores[i], &cpuset);
	}

	// Set affinity
	s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
	if (s != 0) {
		return s;
	}

	// Check affinity
	s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
	if (s != 0) {
	   return s;
	}
	
	return 0;
}

/**
 * Increase scheduling priority of calling thread by moving it to the realtime queue with highest priority.
 * Requires root access.
 *
 * @return 0 if successful, != 0 otherwise
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIThreadAffinity_moveToRealtimeQueue(JNIEnv *p_env, jclass p_class) {
	int s;
	pthread_t thread;
	
	thread = pthread_self();
	
	// Increase priority of pinned thread. We have to move this thread to the realtime queue as SCHED_OTHER's priority cannot be modified
	struct sched_param params;
	params.sched_priority = sched_get_priority_max(SCHED_FIFO); // If this causes problems, set to smaller value between 1 and 99
	s = pthread_setschedparam(thread, SCHED_FIFO, &params); // Requires root access
	if (s != 0) {
		return s;     
	}
	
	return 0;
}
