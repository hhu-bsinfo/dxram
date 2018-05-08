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
#include <assert.h>
#include <pthread.h>
#include <semaphore.h>

#include <errno.h>

// Use predefined blocksize for a first test - to be improved later
#define BLOCKSIZE 4096


/*
 * Class:     de_hhu_bsinfo_dxutils_jni_JNIFileDirect
 * Method:    open
 * opens a new logfile or creates it if it doesnt exist
 * jpath:   complete path to log including the filename
 * mode:    0 -> read/write, 1 -> read only
 * size:    preallocate disk space if != 0
 * return:  the filedescriptor or negative value on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIFileDirect_open(JNIEnv *env, jclass clazz, jstring jpath,
    jint mode, jlong size) {

    // Convert jstring path to pointer
    const jbyte *path;
    path = (*env)->GetStringUTFChars(env, jpath, NULL);

    int fileID = -1;

    // open file use O_SYNC to make sure that all writes and reads are synchronous,
    // so that data is persistent on disk on return
    // use O_DIRECT to bypass Page Cache and get direct access to device
    // O_CREAT: create file if it does not exist
    if(mode == 0) {
        // open for read/write and create if does not exist -> O_RDWR
        // 0666 -> permission mask if file is created, access rights for all users
        fileID = open(path, O_CREAT|O_RDWR|O_DSYNC|O_DIRECT, 0666);
        if (size > 0) {
            if (fallocate(fileID, 0, 0, size) < 0) {
                printf("fallocate not supported by file system! Using ftruncate (space not actually allocated).\n");
                if (ftruncate(fileID, size) < 0) {
                    // Error
                    return -1;
                }
            }
        }
    } else {
      // open for read only and do not create -> O_RDONLY
      fileID = open(path, O_RDONLY|O_DSYNC|O_DIRECT);
    }

    // release String-reference
    (*env)->ReleaseStringUTFChars(env, jpath, path);

    // return file-ID
    return (jint)fileID;
}

/*
 * Class:     de_hhu_bsinfo_dxutils_jni_JNIFileDirect
 * Method:    close
 * closes an open logfile
 * fileID: the file descriptor of the logfile
 * return: 0 on success, -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIFileDirect_close(JNIEnv *env, jclass clazz, jint fileID) {

    int ret = close(fileID);

    return (jint)ret;
}

/*
 * Get length of file
 */
long get_length(int fileID) {

    // save current position
    long current = lseek(fileID, 0, SEEK_CUR);
    // get length of file
    long ret = lseek(fileID, 0, SEEK_END);
    // set current position back
    lseek(fileID, current, SEEK_SET);

    return ret;
}

/*
 * Copy overlapping bytes in front of the buffer
 */
long retain_preceding_bytes(int fileID, long write_buffer, long buffer_offset, long length, long aligned_start_pos,
    int off_start_pos) {

    // Determine start of previous page in buffer
    long block_start = write_buffer + buffer_offset - BLOCKSIZE;
    if (buffer_offset % BLOCKSIZE != 0) {
        block_start -= buffer_offset % BLOCKSIZE;
    }

    // Read bytes from disk
    int read_bytes;
    while (1) {
        read_bytes = pread(fileID, (void*) block_start, BLOCKSIZE, aligned_start_pos);
        if (read_bytes > 0) {
            // We want to read less than a page -> either all or nothing is read
            break;
        } else if (read_bytes == -1) {
            // Error
            return -1;
        }
    }

    // Move bytes to write at the end of the copied bytes
    memmove((void*) (block_start + off_start_pos), (void*) (write_buffer + buffer_offset), length);

    return block_start;
}

/*
 * Class:     de_hhu_bsinfo_dxutils_jni_JNIFileDirect
 * Method:    appendAndTruncate
 * appends buffer (not page-aligned) to file and truncates file afterwards
 * fileID:    the file descriptor
 * buffer:    reference to the byte buffer containing the data; buffer must be page-aligned
 * offset:    start-offset in data-buffer
 * length:    number of bytes to write
 * pos:       write-position in file or -1 for append
 * retain_end: whether overlapping bytes can be overwritten (0) or not (1)
 * set_file_length: whether the file length must be set after writing (0) or not (1)
 * return:    0 on success or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIFileDirect_write(JNIEnv *env, jclass clazz, jint fileID,
    jlong buffer, jint buffer_offset, jint length, jlong pos, jbyte retain_end, jbyte set_file_length) {

    if (pos == -1) {
        // This is a file append -> determine file length and use as pos
        pos = get_length(fileID);
    }

    // Calculate positions for begin and end of access with aligned startposition
    long aligned_start_pos = pos - (pos % BLOCKSIZE);
    long end_pos = pos + length - 1;
    long aligned_end_pos = end_pos + (BLOCKSIZE - (end_pos % BLOCKSIZE)) - 1;

    long aligned_length = aligned_end_pos - aligned_start_pos + 1;

    assert(aligned_length >= length);


    long block_start = buffer;
    if (pos % BLOCKSIZE != 0) {
        // The file start position is not page-aligned -> copy overlapping bytes
        // in front of the buffer aligned at previous page
        // Given buffer MUST have one additional reserved preceding page!
        block_start = retain_preceding_bytes(fileID, buffer, buffer_offset, length, aligned_start_pos, pos % BLOCKSIZE);
        if (block_start == -1) {
            // Error
            return -1;
        }
    } else if (buffer_offset % BLOCKSIZE) {
        // Even though the buffer is page-aligned, the beginning of the write access is not
        // -> move to the beginning of the previous page
        // (buffers are allocated with at least one overlapping page on both sides)
        long write_pos = buffer + buffer_offset;
        block_start = (write_pos - (write_pos % BLOCKSIZE));
        memmove((void*) block_start, (void*) write_pos, length);
    }

    // Write the data from buffer to file
    int bytes_to_write = aligned_length;
    long current_position_in_buffer = block_start;
    long current_position_in_file = aligned_start_pos;
    int written_bytes;
    while (bytes_to_write > 0) {
        written_bytes = pwrite(fileID, (void*) current_position_in_buffer, bytes_to_write, current_position_in_file);
        if (written_bytes == -1 || written_bytes == 0) {
            // Error or EOF
            return -1;
        }

        current_position_in_buffer += written_bytes;
        current_position_in_file += written_bytes;
        bytes_to_write -= written_bytes;
    }

    if (set_file_length == 1) {
        // Set length of file -> remove padding
        if (ftruncate(fileID, pos + length) == -1) {
            return -1;
        }
    }

    return written_bytes;
}

/*
 * Class:     de_hhu_bsinfo_dxutils_jni_JNIFileDirect
 * Method:    read
 * reads from logfile into data-buffer at current position
 * fileID:    the file descriptor
 * buffer:      reference to the byte buffer that should receive the data
 * offset:    start-offset in data-buffer
 * length:    number of bytes to read
 * w_ptr:     address of preallocated readbuffer
 * w_length:  length of preallocated readbuffer
 * return:    0 on success or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIFileDirect_read(JNIEnv *env, jclass clazz, jint fileID,
    jlong buffer, jint offset, jint length, jlong pos) {

    /* Every read-access with O_DIRECT has to be block-aligned.
    Therefore, we get the boundaries for this access and extend them to block-aligned
    boundaries. */

    // Calculate positions for begin and end of access with aligned startposition
    long aligned_start_pos = pos - (pos % BLOCKSIZE);
    long end_pos = pos + length - 1;
    long aligned_end_pos = end_pos + (BLOCKSIZE - (end_pos % BLOCKSIZE)) - 1;

    long aligned_length = aligned_end_pos - aligned_start_pos + 1;

    assert(aligned_length >= length);
    assert(aligned_start_pos == pos);

    // Read the data from file to buffer
    int bytes_to_read = aligned_length;
    long current_position_in_buffer = buffer;
    long current_position_in_file = aligned_start_pos;
    int read_bytes;
    while (bytes_to_read > 0) {
        read_bytes = pread(fileID, (void*) current_position_in_buffer, bytes_to_read, current_position_in_file);
        if (read_bytes == -1) {
            // Error
            return -1;
        }

        if (read_bytes == 0 || read_bytes % BLOCKSIZE != 0) {
            // EOF
            break;
        }

        current_position_in_buffer += read_bytes;
        current_position_in_file += read_bytes;
        bytes_to_read -= read_bytes;
    }

    return aligned_length - bytes_to_read;
}

/*
 * Class:     de_hhu_bsinfo_dxutils_jni_JNIFileDirect
 * Method:    seek
 * sets the filepointer to given value, measured from beginning of the file
 * fileID:  the file descriptor
 * offset:  new position in file
 * return:  new position in file or -1 on error
 */
JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIFileDirect_seek(JNIEnv *env, jclass clazz, jint fileID,
    jlong offset) {

    int ret = lseek(fileID, offset, SEEK_SET);

    return (jlong)ret;
}

/*
 * Class:     de_hhu_bsinfo_dxutils_jni_JNIFileDirect
 * Method:    length
 * returns the length of the file
 * fileID:  file descriptor
 * return:  the length
 */
JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIFileDirect_length(JNIEnv *env, jclass clazz, jint fileID) {
    return (jlong) get_length(fileID);
}

/*
 * Class:     de_hhu_bsinfo_dxutils_jni_JNIFileDirect
 * Method:    setFileLength
 * sets the length of the file to a given length - padding with 0
 * fileID:  file descriptor
 * length:  new length of the file
 * return:  0 on success, -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_dxutils_jni_JNIFileDirect_setFileLength(JNIEnv *env, jclass clazz,
    jint fileID, jlong length) {
    // assume that given file length is a multiple of BLOCKSIZE
    // Set filelength to block-aligned length
    int ret = ftruncate(fileID, length);

    return (jint)ret;
}
