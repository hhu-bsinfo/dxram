/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

// Compile: gcc -O2 -m64 -o ../libJNIFileDirect.so -fpic -shared -I/usr/lib/jvm/java-8-oracle/include/ -I/usr/lib/jvm/java-8-oracle/include/linux ./JNIFileDirect.c

/* This is the implementation of access to logfiles via JNI and O_DIRECT, so that
 * kernelbuffers in Linux are bypassed and data is written directly. Keep in
 * mind that all addresses have to be algined to a given BLOCKSIZE (here
 * 4096 Byte for SSD) and that the length of data has to be a multiple of
 * this BLOCKSIZE.

 * Created for DXRAM by Christian Gesse, 2016 */

#define _GNU_SOURCE   // include several standards
#include <jni.h>      // needed for JNI
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

// Use predefined blocksize for a first test - to be improved later
#define BLOCKSIZE 4096


/* 
 * Function that calculates algined boundaries, allocates buffer if needed and writes
 * the data to disk. Is called by JNI-functions write and dwrite.
 * Explanations for parameters look at write/dwrite
 */
int writeData(JNIEnv *env, jint fileID, jbyteArray data, jint offset, jint length, jlong pos, jlong w_ptr, jint w_length)
  {
    /* Every write-access with O_DIRECT has to be block-aligned.
       Therefore, we get the boundaris for this access and extend them to block-aligned
       boundaries. To make sure that no existing data is overwritten, we have to read
       the additional blocks from the file. */

     // pointer to writebuffer
     char* w_buf;
     // flag to check if new buffer was allocated
     char buf_created = 0;

     // get current position in file where write-access starts
     long start_pos = lseek(fileID, pos, SEEK_SET);
     // get end position for write access
     long end_pos = start_pos + length - 1;

     // get offsets to next block-aligned positions in file
     int off_start = start_pos % BLOCKSIZE;
     int off_end = BLOCKSIZE - (end_pos % BLOCKSIZE);

     // calculate positions for begin and end of access with aligned startposition
     long aligned_start_pos = start_pos - off_start;
     long aligned_end_pos = end_pos + off_end - 1;
     long aligned_length = aligned_end_pos - aligned_start_pos + 1;

     // check, if preallocated buffer fits, otherwise allocate buffer
     if((void*)w_ptr != NULL && aligned_length <= w_length)
     {
       w_buf = (char*) w_ptr;
     }else{
       if(posix_memalign((void**) &w_buf, BLOCKSIZE, aligned_length)){
         return -1;
       }
       buf_created = 1;
     }

     // read data from file in extended areas
     // if start-offset is 0, then the start adress is already aligned and nothing needs to be read
     if(off_start){
       pread(fileID, (void*) w_buf, BLOCKSIZE, aligned_start_pos);
     }
     // if it is only 1 byte to next blocksize-aligned position, then end_pos+1 would be block-aligned
     // -> so end_pos is the end of the block before and the write access reaches until end_pos
     // -> no data needs to be read because it would be overwritten by write access
     if(off_end != 1){
       pread(fileID, (void*) (w_buf + aligned_length - BLOCKSIZE), BLOCKSIZE, aligned_end_pos - BLOCKSIZE + 1);
     }

     // read the bytes from data that should be written
     // use of GetByteArrayRegion because data needs to be copied to an aligned writebuffer nevertheless
     // -> so direct pointer to data via GetPrimitiveArrayCritical is not helpful
     (*env)->GetByteArrayRegion(env, data, offset, length, w_buf + off_start);
     // write the data from buffer to file
     int ret = pwrite(fileID, (void*) w_buf, aligned_length, aligned_start_pos);
     // set current position in file
     lseek(fileID, length, SEEK_CUR);

     // free the buffer if it was new allocated
     if(buf_created)
     {
       free(w_buf);
     }
     
     return ret;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    open
 * Opens a new logfile or creates it if it doesnt exist
 * jpath:   complete path to log including the filename
 * mode:    0 -> read/write, 1 -> read only
 * return:  the filedescriptor or negative value on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_open
  (JNIEnv *env, jclass clazz, jstring jpath, jint mode)
  {
    // Convert jstring path to pointer
    const jbyte *path;
    path = (*env)->GetStringUTFChars(env, jpath, NULL);

    int fileID = -1;

    // open file use O_SYNC to make sure that all writes and reads are synchronous,
    // so that data is persistent on disk on return
    // use O_DIRECT to bypass Page Cache and get direct access to device
    // O_CREAT: create file if it does not exist
    if(mode == 0){
      // open for read/write and create if does not exist -> O_RDWR
      // 0666 -> permission mask if file is created, access rights for all users
      fileID = open(path, O_CREAT|O_RDWR|O_SYNC|O_DIRECT, 0666);
    }else{
      // open for read only and do not create -> O_RDONLY
      fileID = open(path, O_RDONLY|O_SYNC|O_DIRECT);
    }


    // release String-reference
    (*env)->ReleaseStringUTFChars(env, jpath, path);

    // return file-ID
    return  (jint)fileID;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    createBuffer
 * creates a new aligned buffer in memory - this buffer can be used for buffering in
 * read/write operations instead of allocating new buffers. Notice that you have to use different buffers for
 * read and write since they are not synchronized.
 * size:    length of buffer in bytes, must be a multiple of BLOCKSIZE
 * return:  address (pointer) of the created buffer or NULL
 */
JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_createBuffer
  (JNIEnv *env, jclass clazz, jint size)
  {
    char* ptr = 0;
    posix_memalign((void**) &ptr, BLOCKSIZE, size);

    return (jlong)ptr;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    freeBuffer
 * frees a created buffer - use for read/write buffers from createBuff
 * ptr:   address of the buffer, interpreted as a pointer
 */
JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_freeBuffer
  (JNIEnv *env, jclass clazz, jlong ptr)
  {
    free((void*) ptr);
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    close
 * closes an open logfile
 * fileID: the file descriptor of the logfile
 * return: 0 on success, -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_close
  (JNIEnv *env, jclass clazz, jint fileID)
  {
    int ret = close(fileID);
    return (jint)ret;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    write
 * writes to the file from buffer data at current position
 * fileID:    the file descriptor
 * data:      reference to the byte buffer containing the data
 * offset:    start-offset in data-buffer
 * length:    number of bytes to write
 * w_ptr:     address of preallocated writebuffer
 * w_length:  length of preallocated writebuffer
 * return:    0 on success or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_write
  (JNIEnv *env, jclass clazz, jint fileID, jbyteArray data, jint offset, jint length, jlong pos, jlong w_ptr, jint w_length)
  {
     int ret = writeData(env, fileID, data, offset, length, pos, w_ptr, w_length);
     return (jint)ret;

  }
  
/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    dwrite
 * writes to the file from buffer data at current position and sets file length -> use for VersionLog because file is dynamically growing and only appended
 * fileID:    the file descriptor
 * data:      reference to the byte buffer containing the data
 * offset:    start-offset in data-buffer
 * length:    number of bytes to write
 * w_ptr:     address of preallocated writebuffer
 * w_length:  length of preallocated writebuffer
 * return:    0 on success or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_dwrite
  (JNIEnv *env, jclass clazz, jint fileID, jbyteArray data, jint offset, jint length, jlong pos, jlong w_ptr, jint w_length)
  {
     // get current position in file where write-access starts - because this value is used later in this function
     long start_pos = lseek(fileID, pos, SEEK_SET);
     
     // write the data to disk
     int ret = writeData(env, fileID, data, offset, length, pos, w_ptr, w_length);
     
     // set length of file - cut away the padding that has been added because of alignement
     // this operation works for VersionLogs, because there new data is only appended
     ftruncate(fileID, start_pos + length);
     
     return (jint)ret;

  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    read
 * reads from logfile into data-buffer at current position
 * fileID:    the file descriptor
 * data:      reference to the byte buffer that should receive the data
 * offset:    start-offset in data-buffer
 * length:    number of bytes to read
 * w_ptr:     address of preallocated readbuffer
 * w_length:  length of preallocated readbuffer
 * return:    0 on success or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_read
  (JNIEnv *env, jclass clazz, jint fileID, jbyteArray data, jint offset, jint length, jlong pos, jlong r_ptr, jint r_length)
  {
     /* Every read-access with O_DIRECT has to be block-aligned.
       Therefore, we get the boundaris for this access and extend them to block-aligned
       boundaries. */

     // pointer to readbuffer
     char* r_buf;
     // flag to check if buffer was created
     char buf_created = 0;

     // get current position in file where read-access starts
     long start_pos = lseek(fileID, pos, SEEK_SET);
     // get end position for read access
     long end_pos = start_pos + length - 1;

     // get offsets to next block-aligned positions in file
     int off_start = start_pos % BLOCKSIZE;
     int off_end = BLOCKSIZE - (end_pos % BLOCKSIZE);

     // calculate positions for begin and end of read access with aligned startposition and length
     long aligned_start_pos = start_pos - off_start;
     long aligned_end_pos = end_pos + off_end - 1;
     long aligned_length = aligned_end_pos - aligned_start_pos + 1;

     // Allocate aligned read buffer
     if((void*)r_ptr != NULL && aligned_length <= r_length)
     {
       r_buf = (char*) r_ptr;
     }else{
       if(posix_memalign((void**) &r_buf, BLOCKSIZE, aligned_length)){
         return -1;
       }
       buf_created = 1;
     }

     // read the data from file to buffer
     int ret = pread(fileID, (void*) r_buf, aligned_length, aligned_start_pos);

     // write the bytes to buffer
     // use of SetByteArrayRegion has same reason as in write (look there for details)
     (*env)->SetByteArrayRegion(env, data, offset, length, r_buf + off_start);

     // set current position in file
     lseek(fileID, length, SEEK_CUR);

     // free allocated buffer
     if (buf_created) {
       free(r_buf);
     }

     return (jint)ret;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    seek
 * sets the filepointer to given value, measured from beginning of the file
 * fileID:  the file descriptor
 * offset:  new position in file
 * return:  new position in file or -1 on error
 */
JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_seek
  (JNIEnv *env, jclass clazz, jint fileID, jlong offset)
  {
    int ret = lseek(fileID, offset, SEEK_SET);
    return (jlong)ret;
  }


/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    length
 * return length of the file
 * fileID:  the descriptor of the fileID
 * return:  the length of the file
 */
JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_length
  (JNIEnv *env, jclass clazz, jint fileID)
  {
    // save current position
    long current = lseek(fileID, 0, SEEK_CUR);
    // get length of file
    long ret = lseek(fileID, 0, SEEK_END);
    // set current position back
    lseek(fileID, current, SEEK_SET);
    
    return (jlong) ret;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    setFileLength
 * sets the length of the file to a given length - padding with 0
 * fileID:  file descriptor
 * length:  new length of the file
 * return:  0 on success, -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_setFileLength
  (JNIEnv *env, jclass clazz, jint fileID, jlong length)
  {
    // assume that given file length is a multiple of BLOCKSIZE
    // Set filelength to block-aligned length
    int ret = ftruncate(fileID, length);
    return (jint)ret;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileDirect
 * Method:    test
 * a simple testfunction to check if jni-lib is loaded successful
 */
JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNIFileDirect_test
  (JNIEnv *env, jclass clazz)
  {
    printf("%s\n", "This is a JNI-O_DIRECT-test!");
    fflush(stdout);
  }
