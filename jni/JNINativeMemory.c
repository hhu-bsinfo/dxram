// compile:
// linux:
// gcc -O2 -shared -fpic -o libJNINativeMemory.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c
// mac:
// gcc -O2 -shared -fpic -o libJNINativeMemory.dylib -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include/darwin JNINativeMemory.c

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>

// requres xcode command tools to be installed on mac osx
#ifdef __APPLE__
#include <machine/endian.h>
#else
// that's for linux
#include <endian.h>
#endif

#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define __bswap_16 OSSwapInt16
#define __bswap_32 OSSwapInt32
#define __bswap_64 OSSwapInt64
#else
#include <byteswap.h>
#endif

// important note: in your java application, make sure to cover the following situation correctly:
// for example, if you write single longs (or a primitive long array) using this class to native memory
// and you read it back using a byte array which you put into a byte buffer, keep in mind that the data
// read into the byte buffer has the same endianess as your system (i.e. most likely little) but
// in your java code, you operate on big endian(ess). Make sure to set the byte order when using a 
// ByteBuffer to nativeOrder. Otherwise you read corrupted data.

JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_alloc(JNIEnv *p_env, jclass p_class, jlong p_size) 
{
	void* mem = malloc(p_size);
	if (mem == NULL)
		return 0;

	//printf("alloc %d %p\n", p_size, mem);

	return (jlong) mem;
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_free(JNIEnv *p_env, jclass p_class, jlong p_addr) 
{	
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return;
	}

	//printf("free %p\n", p_addr);

	free((void*) p_addr);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_dump(JNIEnv *p_env, jclass p_class, jlong p_addr, jlong p_length, jstring p_path) 
{	
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return;
	}
	
	const char* path = (*p_env)->GetStringUTFChars(p_env, p_path, NULL);

	FILE* file = fopen(path, "w+");
	if (file == NULL)
	{
		printf("Opening file %s for memory dump of %p failed.", path, (void*) p_length);
		return;	
	}

	uint8_t buffer[16384];
	void* pos = (void*) p_addr;
	while (p_length > 0)
	{
		uint32_t readSize = sizeof(buffer);
		if (p_length < readSize)
		{
			readSize = p_length;
			p_length = 0;
		}
		else
			p_length -= readSize;

		memcpy(buffer, pos, readSize);
		pos += readSize;

		if (fwrite(buffer, readSize, 1, file) != 1)
		{
			printf("Writing to output file %s failed: %d\n", path, errno);
			return;
		}
	}
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_set(JNIEnv *p_env, jclass p_class, jlong p_addr, jbyte p_value, jlong p_size) 
{
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return;
	}

	memset((void*) p_addr, p_value, p_size);	
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_copy(JNIEnv *p_env, jclass p_class, jlong p_addrDest, jlong p_addrSrc, jlong p_size) 
{
	if (p_addrDest == 0 || p_addrSrc == 0)
	{
		printf("Native memory NULL pointer.\n");
		return;
	}

	memcpy((void*) p_addrDest, (void*) p_addrSrc, p_size);		
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_write(JNIEnv *p_env, jclass p_class, jlong p_addr, jbyteArray p_array, jint p_arrayOffset, jint p_length) 
{
	jbyte* array = (*p_env)->GetByteArrayElements(p_env, p_array, NULL);

	memcpy((void*) p_addr, (void*) (array + p_arrayOffset), p_length);

	(*p_env)->ReleaseByteArrayElements(p_env, p_array, array, 0);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_writeShorts(JNIEnv *p_env, jclass p_class, jlong p_addr, jshortArray p_array, jint p_arrayOffset, jint p_length) 
{
	jshort* array = (*p_env)->GetShortArrayElements(p_env, p_array, NULL);

	memcpy((void*) p_addr, (void*) (array + p_arrayOffset), p_length * sizeof(jshort));

	(*p_env)->ReleaseShortArrayElements(p_env, p_array, array, 0);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_writeInts(JNIEnv *p_env, jclass p_class, jlong p_addr, jintArray p_array, jint p_arrayOffset, jint p_length) 
{
	jint* array = (*p_env)->GetIntArrayElements(p_env, p_array, NULL);

	memcpy((void*) p_addr, (void*) (array + p_arrayOffset), p_length * sizeof(jint));

	(*p_env)->ReleaseIntArrayElements(p_env, p_array, array, 0);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_writeLongs(JNIEnv *p_env, jclass p_class, jlong p_addr, jlongArray p_array, jint p_arrayOffset, jint p_length) 
{
	jlong* array = (*p_env)->GetLongArrayElements(p_env, p_array, NULL);

	memcpy((void*) p_addr, (void*) (array + p_arrayOffset), p_length * sizeof(jlong));

	(*p_env)->ReleaseLongArrayElements(p_env, p_array, array, 0);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_read(JNIEnv *p_env, jclass p_class, jlong p_addr, jbyteArray p_array, jint p_arrayOffset, jint p_length) 
{
	jbyte* array = (*p_env)->GetByteArrayElements(p_env, p_array, NULL);

	memcpy((void*) (array + p_arrayOffset), (void*) p_addr, p_length);

	(*p_env)->ReleaseByteArrayElements(p_env, p_array, array, 0);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_readShorts(JNIEnv *p_env, jclass p_class, jlong p_addr, jshortArray p_array, jint p_arrayOffset, jint p_length) 
{
	jshort* array = (*p_env)->GetShortArrayElements(p_env, p_array, NULL);

	memcpy((void*) (array + p_arrayOffset), (void*) p_addr, p_length * sizeof(jshort));

	(*p_env)->ReleaseShortArrayElements(p_env, p_array, array, 0);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_readInts(JNIEnv *p_env, jclass p_class, jlong p_addr, jintArray p_array, jint p_arrayOffset, jint p_length) 
{
	jint* array = (*p_env)->GetIntArrayElements(p_env, p_array, NULL);

	memcpy((void*) (array + p_arrayOffset), (void*) p_addr, p_length * sizeof(jint));

	(*p_env)->ReleaseIntArrayElements(p_env, p_array, array, 0);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_readLongs(JNIEnv *p_env, jclass p_class, jlong p_addr, jlongArray p_array, jint p_arrayOffset, jint p_length) 
{
	jlong* array = (*p_env)->GetLongArrayElements(p_env, p_array, NULL);

	memcpy((void*) (array + p_arrayOffset), (void*) p_addr, p_length * sizeof(jlong));

	(*p_env)->ReleaseLongArrayElements(p_env, p_array, array, 0);
}

JNIEXPORT jbyte JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_readByte(JNIEnv *p_env, jclass p_class, jlong p_addr) 
{
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return 0;
	}

	return *((jbyte*) p_addr);	
}

JNIEXPORT jshort JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_readShort(JNIEnv *p_env, jclass p_class, jlong p_addr) 
{
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return 0;
	}

	return *((jshort*) p_addr);
}

JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_readInt(JNIEnv *p_env, jclass p_class, jlong p_addr) 
{
	/*
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return 0;
	}
	*/

	return *((jint*) p_addr);
}

JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_readLong(JNIEnv *p_env, jclass p_class, jlong p_addr) 
{
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return 0;
	}

	return *((jlong*) p_addr);
}

JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_readValue(JNIEnv *p_env, jclass p_class, jlong p_addr, jint p_byteCount) 
{
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return 0;
	}

	// clip
	if (p_byteCount > 8)
		p_byteCount = 8;

	jlong ret = 0;
	memcpy((void*) &((uint8_t*) &ret)[8 - p_byteCount], (void*) p_addr, p_byteCount);
	// because this is never used with reading from an array of values
	// we have to swap the order on little endian to deliver the correct value
	return __bswap_64(ret);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_writeByte(JNIEnv *p_env, jclass p_class, jlong p_addr, jbyte p_value) 
{
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return;
	}

	*((jbyte*) p_addr) = p_value;
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_writeShort(JNIEnv *p_env, jclass p_class, jlong p_addr, jshort p_value) 
{
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return;
	}

	*((jshort*) p_addr) = p_value;
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_writeInt(JNIEnv *p_env, jclass p_class, jlong p_addr, jint p_value) 
{
	/*
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return;
	}
	*/

	*((jint*) p_addr) = p_value;
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_writeLong(JNIEnv *p_env, jclass p_class, jlong p_addr, jlong p_value) 
{
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return;
	}

	*((jlong*) p_addr) = p_value;
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNINativeMemory_writeValue(JNIEnv *p_env, jclass p_class, jlong p_addr, jlong p_value, jint p_byteCount) 
{
	if (p_addr == 0)
	{
		printf("Native memory NULL pointer.\n");
		return;
	}

	// clip
	if (p_byteCount > 8)
		p_byteCount = 8;

	// because this is never used with reading from an array of values
	// we have to swap the order on little endian to deliver the correct value
	p_value = __bswap_64(p_value);
	memcpy((void*) p_addr, (void*) &((uint8_t*)&p_value)[8 - p_byteCount], p_byteCount);
}







