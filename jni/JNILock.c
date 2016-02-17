// compile:
// linux:
// gcc -shared -fpic -o libJNILock.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNILock.c
// mac:
// gcc -shared -fpic -o JNIconsole.dylib -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include/darwin JNILock.c

#include <jni.h>
#include <stdio.h>


#define READER_BITMASK 0x7FFFFFFF
#define WRITER_FLAG 0x80000000

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_locks_JNILock_readLock(JNIEnv *p_env, jclass p_class, jlong p_address) {
	int *lock;
	int oldValue;
	int newValue;

	lock = (int*)p_address;

	while ((*lock & WRITER_FLAG) != 0);

	do {
		oldValue = *lock & READER_BITMASK;
		newValue = oldValue + 1;
	} while (!__sync_bool_compare_and_swap(lock, oldValue, newValue));
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_locks_JNILock_readUnlock(JNIEnv *p_env, jclass p_class, jlong p_address) {
	int *lock;
	int oldValue;
	int newValue;

	lock = (int*)p_address;

	do {
		oldValue = *lock;
		newValue = oldValue - 1;
	} while (!__sync_bool_compare_and_swap(lock, oldValue, newValue));
}

JNIEXPORT jboolean JNICALL Java_de_hhu_bsinfo_utils_locks_JNILock_tryReadLock(JNIEnv *p_env, jclass p_class, jlong p_address) {
	jboolean ret = JNI_FALSE;
	int *lock;
	int oldValue;
	int newValue;

	lock = (int*)p_address;

	if ((*lock & WRITER_FLAG) == 0) {
		oldValue = *lock & READER_BITMASK;
		newValue = oldValue + 1;
		if (__sync_bool_compare_and_swap(lock, oldValue, newValue)) {
			ret = JNI_TRUE;
		}
	}
	
	return ret;
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_locks_JNILock_writeLock(JNIEnv *p_env, jclass p_class, jlong p_address) {
	int *lock;
	int oldValue;
	int newValue;

	lock = (int*)p_address;

	do {
		oldValue = *lock & READER_BITMASK;
		newValue = oldValue | WRITER_FLAG;
	} while (!__sync_bool_compare_and_swap(lock, oldValue, newValue));

	while ((*lock & READER_BITMASK) != 0);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_locks_JNILock_writeUnlock(JNIEnv *p_env, jclass p_class, jlong p_address) {
	int *lock;

	lock = (int*)p_address;

	__sync_val_compare_and_swap(lock, WRITER_FLAG, 0);
}

JNIEXPORT jboolean JNICALL Java_de_hhu_bsinfo_utils_locks_JNILock_tryWriteLock(JNIEnv *p_env, jclass p_class, jlong p_address) {
	jboolean ret = JNI_FALSE;
	int *lock;

	lock = (int*)p_address;

	if (__sync_bool_compare_and_swap(lock, 0, WRITER_FLAG)) {
		ret = JNI_TRUE;
	}

	return ret;
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_locks_JNILock_lock(JNIEnv *p_env, jclass p_class, jlong p_address) {
	int *lock;

	lock = (int*)p_address;

	while (!__sync_bool_compare_and_swap(lock, 0, 1));
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_locks_JNILock_unlock(JNIEnv *p_env, jclass p_class, jlong p_address) {
	int *lock;

	lock = (int*)p_address;

	__sync_val_compare_and_swap(lock, 1, 0);
}

JNIEXPORT jboolean JNICALL Java_de_hhu_bsinfo_utils_locks_JNILock_tryLock(JNIEnv *p_env, jclass p_class, jlong p_address) {
	jboolean ret = JNI_FALSE;
	int *lock;

	lock = (int*)p_address;

	if (__sync_bool_compare_and_swap(lock, 0, 1)) {
		ret = JNI_TRUE;
	}

	return ret;
}
