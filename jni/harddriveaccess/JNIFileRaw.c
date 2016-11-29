// Compile: gcc -O2 -m64 -o ../libJNIFileRaw.so -fpic -shared -I/usr/lib/jvm/java-8-oracle/include/ -I/usr/lib/jvm/java-8-oracle/include/linux ./JNIFileRaw.c

/* This is an implementation of raw-device access bypassing the page cache of linux.
 * All writes and reads go to an unformatted partition of the ssd, mounted a a raw-device.
 * Some of the Macros, structures and definitons used here come from JNIFileRawStructures.h
 *
 * Created for DXRAM by Christian Gesse, 2016 */

#define _GNU_SOURCE   // include several standards
#include <jni.h>      // needed for JNI
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <errno.h>
#include <pthread.h>
#include "JNIFileRawStructures.h"


/* For low-level stuff in index use uintN_t instead of int and long.
   Since java has no simple implementation of unsigned, all parameter from jni are
   normal int and long */

// device number after opening raw-device
int device = -1;
// length of raw device
uint64_t dev_size = 0;
// pointer to position where next log has to be appended
uint64_t append = 0;
// pointer to first index_entry
uint64_t index_start = 0;
// pointer to start of data
uint64_t data_start = 0;
// next free index to use
uint32_t next_free_index = 0;
// index entries per BLOCKSIZE
uint32_t entries_per_block = 0;
// number of available index entries
uint32_t index_length = 0;
// recognition string for logging device_header
char header_begin[] = {"DXRAMdevice"};
// a lock used to synchronize all changes to the index
pthread_mutex_t lock;


// pointer to index in memory
index_entry_t *ind = NULL;


/* 
 * Function to write an index-entry back to disk. Since we are performing 
 * direct disk access, the whole block has to bew written.
 * Index has to be locked before function call!
 * 
 * index_number: number of entry that should be written back
 * return: 0 on success, -1 on error
 */
int writeBackIndex(uint32_t index_number)
  {
    // write changed block of index back to disk - first calculate start of the block
    uint64_t start_write = index_start + (index_number - (index_number % entries_per_block)) * sizeof(index_entry_t);
    if(pwrite(device, (void*) &ind[index_number - (index_number % entries_per_block)], BLOCKSIZE, start_write) == -1){
      // if writing back failed delete entry in index to avoid inconsistency
      ind[index_number].status = 0x00;
      
      return -1;
    }
    return 0;
  }

/*
 * Checks if the given index position AND the enough space after append are avaible
 * Needed if new index_entries should be created
 * Lock on index has to be aquired before calling the function!
 * pos:     index-position to check
 * length:  space needed behind append
 * return: 0 if ok, < 0 if not
 */
int checkNextPosAndSpace(uint32_t pos, uint32_t length)
  {
    // check if index position is available
    if(pos >= index_length){
      printf("Index of rawdevice is full - cannot create new entry\n");
      fflush(stdout);
      return -2;
    }
    // check if device has enough free space
    if(append + length  >= dev_size){
      printf("Rawdevice is full - cannot create new entry\n");
      fflush(stdout);
      return -1;
    }
    // everything ok
    return 0;
  }
  

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    prepareRawDevice
 * prepares the rawdevice for use as logging-device.
 * jpath:   Path to rawdevice-file(e.g. /dev/raw/raw1)
 * mode:    0 -> overwrite exisiting data, 1 -> check for old data and dont overwrite
 * return:  filedescriptor of rawdevice or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_prepareRawDevice
  (JNIEnv *env, jclass clazz, jstring jpath, jint mode)
  {

    // convert path to device
    const jbyte *path;
    path = (*env)->GetStringUTFChars(env, jpath, NULL);

    // init the mutex
    pthread_mutex_init (&lock, NULL);

    // open device - use O_SYNC to make sure that all writes and reads are synchronous,
    // so that data is persistent on disk on return
    // O_RDWR: read and write access to device
    device = open(path, O_RDWR|O_SYNC);

    // create buffer for header of rawdevice
    device_header_t *dev_head = NULL;
    if(posix_memalign((void**) &dev_head, BLOCKSIZE, BLOCKSIZE) != 0){
      close(device);
      return -1;
    }
    // read current header of rawdevice
    pread(device, (void*) dev_head, BLOCKSIZE, 0);

    // read in existing metadata in case that there is existing data
    if(mode == 1 && (strncmp(dev_head->begin, header_begin, 11) == 0)){

      // fill in data from header_begin
      dev_size = dev_head->length;
      index_start = dev_head->index;
      index_length = dev_head->index_length;
      data_start = dev_head->data;
      entries_per_block = dev_head->entries_per_block;

      // Scan for last used index and set next_free_index, set append
      
      pthread_mutex_lock(&lock);

      // allocate aligned buffer to load index data
      if(posix_memalign((void**) &ind, BLOCKSIZE, index_length * sizeof(index_entry_t)) != 0){
        pthread_mutex_unlock(&lock);
        close(device);
        return -1;
      }

      // read index data from device
      pread(device, (void*) ind, index_length * sizeof(index_entry_t), index_start);

      // now look for the first index that is not used
      uint32_t i = 0;
      for(i=0; i < index_length; i++){
        // check if the first bit of status is set
        if(((ind[i].status) & sel_bit_first) == 0x00){
          // set next free index to i because this entry is not in use
          next_free_index = i;
          // calculate new append position
          append = ind[i].begin + ind[i].part_length;
          break;
        }
      }

      pthread_mutex_unlock(&lock);

      printf("Loaded existing log-index.\n");
      printf("The size of this device is %lu MB, the index has %u entries and data begin at %lu.\n", dev_size/(1024*1024), index_length, data_start);
      printf("The next free index is at %u and we have %u entries per block.\n", next_free_index, entries_per_block);
      fflush(stdout);


    }else{  // or create new device and overwrite exisiting data

      // get length of device and align it to BLOCKSIZE
      if(ioctl(device, BLKGETSIZE64, &dev_size) == -1){
        close(device);
        return -1;
      }
      dev_size = dev_size - (dev_size % BLOCKSIZE);

      // set begin of index to 4096 (first 4096 bytes should be used for metadata)
      index_start = BLOCKSIZE;

      // get number of index entries per block
      entries_per_block = BLOCKSIZE / sizeof(index_entry_t);

      // calculate length of index and align it to a multiple of entries_per_block
      // Comments on the formula: First check, how many SecondaryLogs would fit into the partition
      // ignoring that there are also VersionLogs. 
      // Then, multiply by INDEX_FACTOR in assumption that for every SecondaryLog there is need
      // of up to INDEX_FACTOR-1 VersionLog-Chunks with size VER_BLOCK_SIZE
      // This should ensure that there is enough space in the index since it has a fixed size
      // Otherwise, this ensures that the preallocated fix index size is not too big
      // If VersionLogs are growing more than expected, simply increment INDEX_FACTOR or tune VER_BLOCK_SIZE
      // the use of RAM is <2MB even for very big partitions (>2.5TBytes) 
      index_length = ((dev_size / SEC_LOGSIZE)*INDEX_FACTOR);
      // alignement
      index_length = index_length + (entries_per_block - (index_length % entries_per_block));

      // tprint out several infos
      printf("Created rawdevice with length %lu MB.\n", (long) dev_size/(1024*1024));
      printf("There are %u index-entries per block and %u entries are available.\n", entries_per_block, index_length);
      printf("The size of one index entry is %lu bytes.\n", sizeof(index_entry_t));
      fflush(stdout);

      pthread_mutex_lock(&lock);

      // allocate aligned buffer for index data
      if(posix_memalign((void**) &ind, BLOCKSIZE, index_length * sizeof(index_entry_t)) != 0){
        pthread_mutex_unlock(&lock);
        close(device);
        return -1;
      }

      // fill index buffer with 0 and flush to device at start position of index
      memset((void*) ind, 0, index_length * sizeof(index_entry_t));
      pwrite(device, (void*) ind, index_length * sizeof(index_entry_t), index_start);

      // start point for data
      data_start = index_start + index_length * sizeof(index_entry_t);
      append = data_start;

      // fill header struct with 0
      memset((void*) dev_head, 0, BLOCKSIZE);
      // fill in data for device header and flush to disk
      strcpy(dev_head->begin, header_begin);
      dev_head->length = dev_size;
      dev_head->blocksize = BLOCKSIZE;
      dev_head->index = index_start;
      dev_head->index_length = index_length;
      dev_head->data = data_start;
      dev_head->entry_size = sizeof(index_entry_t);
      dev_head->entries_per_block = entries_per_block;
      // write device header to device
      pwrite(device, (void*) dev_head, BLOCKSIZE, 0);

      pthread_mutex_unlock(&lock);

      printf("Wrote index and header to disk - preparation successfull!\n");
      fflush(stdout);
    }

    free((void*) dev_head);
    (*env)->ReleaseStringUTFChars(env, jpath, path);

    return device;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    createLog
 * create a new log and create index entry
 * jFileName: the filename (and only the name without path!) of the logfile
 * size:      the size of the log (in case of 0 it is an dynamically growing log, use  8MB then for a start)
 * return:    indexnumber of the first block of the logfile
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_createLog
  (JNIEnv *env, jclass clazz, jstring jFileName, jlong size)
  {
    // Convert jstring fileName to pointer
    jbyte *fileName;
    fileName = (*env)->GetStringUTFChars(env, jFileName, NULL);

    // check if it is a primary, secondary or version log
    char logtype = 0x00;
    if(strstr((char*) fileName, prim_indicator) != NULL){
      // primary log
      logtype = 'p';
    }else if(strstr((char*) fileName, sec_indicator) != NULL){
      // secondary log
      logtype = 's';
    }else if(strstr((char*) fileName, ver_indicator) != NULL){
      // version log
      logtype = 'v';
    }else{
      printf("Illegal filename\n");
      return -1;
    }

    // check size for log
    uint32_t logsize;
    if(size == 0){
      logsize = VER_BLOCK_SIZE;
    }else{
      logsize = size;
    }

    // prepare statusbits for index (first bit -> block exists, second bit -> log is open)
    char logstatus = 0xC0;

    // enter critical section because index data is affected
    pthread_mutex_lock(&lock);

    // check if device has enough free space and index position is available
    if(checkNextPosAndSpace(next_free_index, size) < 0){
      pthread_mutex_unlock(&lock);
      return -1;
    }

    // create index entry
    ind[next_free_index].status = logstatus;
    ind[next_free_index].type = logtype;
    ind[next_free_index].begin = append;
    ind[next_free_index].part_length = logsize;
    ind[next_free_index].nextBlock = next_free_index;
    ind[next_free_index].firstBlock = next_free_index;
    ind[next_free_index].cur_length = 0;
    strncpy(ind[next_free_index].logName, (char*) fileName, 37);
    ind[next_free_index].logName[37] = '\0';
    
    if(writeBackIndex(next_free_index) == -1){
      pthread_mutex_unlock(&lock);
      return -1;
    }


    // increment next free index position
    int ret = next_free_index;
    next_free_index++;
    // increase append-pointer
    append += logsize;

    // leave critical section
    pthread_mutex_unlock(&lock);

    printf("Created log with index number %u and name %s.\n", ret, ind[ret].logName);
    fflush(stdout);
    
    (*env)->ReleaseStringUTFChars(env, jFileName, fileName);

    // return index number as filedescriptor
    return ret;

  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    openLog
 * open an existing logfile
 * jFileName: the filename (and only the name wothout path!) of the logfile
 * return:    index of logfile as descriptor and -1 if log was not found or error occured
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_openLog
  (JNIEnv *env, jclass clazz, jstring jFileName)
  {
    // because this function is only used to open a log in recovery, no lock
    // for index reading is needed -> only for changing the index

    // Convert jstring fileName to pointer
    jbyte *fileName;
    fileName = (*env)->GetStringUTFChars(env, jFileName, NULL);

    // search for log with this name
    uint32_t lastIndex = next_free_index;
    int i = 0;
    for(i = 0; i <= lastIndex; i++){
      // if the filename is found and the first status bit is set and it is the first block of the file, we have found the file
      if((strstr((char*) fileName, ind[i].logName) != NULL) && (((ind[i].status) & sel_bit_first) != 0) && (ind[i].firstBlock == i)){
        printf("Opened log with name %s and index %u\n", (char*) fileName, i);
        printf("The size of this log is %u MB.\n", ind[i].part_length/(1024*1024));
        fflush(stdout);

        // set the open bit - no need for page to be written back, 
        // because information is not persistent
        pthread_mutex_lock(&lock);
        ind[i].status = ind[i].status | set_bit_second;
        pthread_mutex_unlock(&lock);
        
        (*env)->ReleaseStringUTFChars(env, jFileName, fileName);

        // return index as descriptor
        return i;
      }
    }
    
    (*env)->ReleaseStringUTFChars(env, jFileName, fileName);

    printf("Log with name %s does not exist.\n", (char*) fileName);
    return -1;
  }


/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    closeLog
 * closes an opened logfile
 * fileID: the ID of start index 
 * return: 0 on success
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_closeLog
  (JNIEnv *env, jclass clazz, jint fileID)
  {
    pthread_mutex_lock(&lock);
    // delete status bit - no need to write back to disk,
    // because this is not persistent
    ind[fileID].status = ind[fileID].status & null_bit_second;

    pthread_mutex_unlock(&lock);
    return 0;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    createBuffer
 * creates a new aligned buffer in memory - this buffer can be used for buffering in
 * read/write operations. Notice that you have to use different buffers for
 * read and write since they are not synchronized 
 * size:    length of buffer in bytes, must be a multiple of BLOCKSIZE
 * return:  address (pointer) of the created buffer or NULL
 */
JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_createBuffer
  (JNIEnv *env, jclass clazz, jint size)
  {
    char* ptr = 0;
    posix_memalign((void**) &ptr, BLOCKSIZE, size);

    return (jlong)ptr;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    freeBuffer
 * frees a created buffer - use for read/write buffers from createBuff
 * ptr:   address of the buffer, interpreted as a pointer
 */
JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_freeBuffer
  (JNIEnv *env, jclass clazz, jlong ptr)
  {
    free((void*) ptr);
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    write
 * writes to a not dynamically growing file from buffer data at an given position
 * fileID:    the file descriptor
 * data:      reference to the byte buffer containing the data
 * offset:    start-offset in data-buffer
 * length:    number of bytes to write
 * write_pos: write-offset in file
 * w_ptr:     address of preallocated writebuffer
 * w_length:  length of preallocated writebuffer
 * return:    0 on success or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_write
  (JNIEnv *env, jclass clazz, jint fileID, jbyteArray data, jint offset, jint length, jlong write_pos, jlong w_ptr, jint w_length)
  {
    /* Note that in raw-mode every access to disk has to be block aligned with block-aligned
     * buffers. Basically, we can implement the access similar to the one
     * with O_DIRECT with the difference, that file management is done by ourselves.
     * So we have to use the fileID to select the index-entry for the file.
     * Then we can read out the start-position and add it to the write-offset. */

    // pointer to writebuffer
    char* w_buf;
    // flag to check if new buffer was allocated
    char buf_created = 0;

    // get current position in file where write-access starts
    long start_pos = write_pos;
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
      pread(device, (void*) w_buf, BLOCKSIZE, aligned_start_pos + ind[fileID].begin);
    }
    // if it is only 1 byte t next blocksize-aligned position, then nothing to do
    if(off_end != 1){
      pread(device, (void*) (w_buf + aligned_length - BLOCKSIZE), BLOCKSIZE, ind[fileID].begin + (aligned_end_pos - BLOCKSIZE + 1));
    }

    // read the bytes from data that should be written
    // use GetByteArrayRegion because data must be in an BLOCKSIZE-aligned buffer
    // to write it. So, if we direct pointer to the data via GetPrimtiveArrayCritical,
    // the data has to be copied nevertheless. In addition to that, the Critical version
    // blocks the GarbageCollector and should therefore be avoided if possible
    (*env)->GetByteArrayRegion(env, data, offset, length, w_buf + off_start);
    // write the data from buffer to file
    int ret = pwrite(device, (void*) w_buf, aligned_length, aligned_start_pos + ind[fileID].begin);


    // free the buffer if it was new allocated
    if(buf_created)
    {
      free(w_buf);
    }
    return (jint)ret;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    read
 * reads from a not dynamically growing file into data-buffer at given position
 * fileID:    the file descriptor
 * data:      reference to the byte buffer that should receive the data
 * offset:    start-offset in data-buffer
 * length:    number of bytes to read
 * read_pos:  read-offset in file
 * w_ptr:     address of preallocated readbuffer
 * w_length:  length of preallocated readbuffer
 * return:    0 on success or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_read
  (JNIEnv *env, jclass clazz, jint fileID, jbyteArray data, jint offset, jint length, jlong read_pos, jlong r_ptr, jint r_length)
  {
    // pointer to readbuffer
    char* r_buf;
    // flag to check if buffer was created
    char buf_created = 0;

    // get current position in file where read-access starts
    long start_pos = read_pos;
    // get end position for read access
    long end_pos = start_pos + length - 1;

    // get offsets to next block-aligned positions in file
    int off_start = start_pos % BLOCKSIZE;
    int off_end = BLOCKSIZE - (end_pos % BLOCKSIZE);

    // calculate positions for begin and end of read access with aligned startposition and length
    long aligned_start_pos = start_pos - off_start;
    long aligned_end_pos = end_pos + off_end - 1;
    long aligned_length = aligned_end_pos - aligned_start_pos + 1;

    // Allocate aligned read buffer ore use existing one
    if((void*)r_ptr != NULL && aligned_length <= r_length)
    {
      r_buf = (char*) r_ptr;
    }else{
      if(posix_memalign((void**) &r_buf, BLOCKSIZE, aligned_length)){
        return -1;
      }
      buf_created = 1;
    }

    // read the bytes to buffer -> add startposition of the file to start-offset in file
    int ret = pread(device, (void*) r_buf, aligned_length, aligned_start_pos + ind[fileID].begin);

    // set the bytes in buffer - see in write() for reasons why using Set/GetByteArrayRegion
    (*env)->SetByteArrayRegion(env, data, offset, length, r_buf + off_start);

    // free allocated buffer
    if (buf_created) {
      free(r_buf);
    }

    return (jint)ret;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    dwrite
 * writes to a dynamically growing file from buffer data at an given position
 * fileID:    the file descriptor
 * data:      reference to the byte buffer containing the data
 * offset:    start-offset in data-buffer
 * length:    number of bytes to write
 * write_pos: write-offset in file
 * w_ptr:     address of preallocated writebuffer
 * w_length:  length of preallocated writebuffer
 * return:    0 on success or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_dwrite
  (JNIEnv *env, jclass clazz, jint fileID, jbyteArray data, jint offset, jint length, jlong write_pos, jlong w_ptr, jint w_length)
  {
    // pointer to writebuffer
    char* w_buf;
    // flag to check if new buffer was allocated
    char buf_created = 0;

    // get current position in file where write-access starts
    long start_pos = write_pos;
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

    uint32_t start_index_entry = 0;       // index of part contsining aligned_start_pos
    uint32_t end_index_entry = 0;         // index of part containing aligned_end_pos
    uint32_t length_at_begin = 0;         // in-file-offset of the beginning of the start_index-part
    uint32_t length_at_end = 0;           // in-file-offset of the beginning of the end_index-part
    uint32_t cur_ind = fileID;            // index to scan
    uint32_t rem_length = aligned_length;   // length that is not placed in a part yet
    uint32_t in_file_pos = 0;             // current position in file
    uint64_t start_index_write = 0;       // position to write changed index back


    // first step: check file size an add parts if needed
    // this is only index operation without writing the data, because a lock is required to do this
    pthread_mutex_lock(&lock);
    
    /* This is the loop that checks if there are enough parts allocated for this
     * VersionLog to write all the data at the given position.
     * rem_length counts how many bytes can be stored in the already scanned area,
     * if there are no remaining bytes left, the loop finishes and the function continues.
     * Because of the many cases that can occur, loop has many branches with if-else.
     * Therefore, a small overview is given here:
     * 
     * while bytes are remaining
     *   ->if write-start position is in current part
     *       ->if write-end position is in current part
     *           nothing else to do, write_end position exists -> no remaining bytes left
     *       ->else (write_end position not in current part)
     *           ->if next part exists
     *               goto next part -> next round
     *           ->else (there exists no next part)
     *              a new part mus be allcated for all remaining data -> no remaining bytes left
     *   ->else (write-start position not in current part)
     *       ->if write-end position is in this part
     *           nothing else to do, write-end position exists -> no remaining bytes left
     *       ->else (write-end position is not in this part)     
     *           ->if write-start position is surpassed
     *              remaining length has to be updated
     *           ->if next part exists
     *              goto next part -> next round
     *           ->else (there is no next part)
     *              a new part has to be created
     *              ->if write-start position was not surpassed
     *                 add offset from current to write-start position to the remaining length
     *              create part with remaining length -> no remaining bytes left 
     * 
     * This scheme should guide you through the following loop:
     */              

    while(rem_length > 0){
      // start position for write is in current part
      if(aligned_start_pos >= in_file_pos && aligned_start_pos < in_file_pos + ind[cur_ind].part_length){
        
        // set start_index
        start_index_entry = cur_ind;
        length_at_begin = in_file_pos;

        // end position is in current part -> nothing to do
        if(aligned_end_pos < in_file_pos + ind[cur_ind].part_length){
          end_index_entry = cur_ind;
          length_at_end = in_file_pos;
          rem_length = 0;
        // search for another part or add an new part
        }else{
          // set position to end of part and calculate remaining length to write
          in_file_pos += ind[cur_ind].part_length;
          rem_length -= (in_file_pos - aligned_start_pos);

          // case 1: there is a next part
          if(ind[cur_ind].nextBlock != cur_ind){
            // scan next part
            cur_ind = ind[cur_ind].nextBlock;
          // case 2: there exists no further part -> allocate space for a new part 
          // that is big enough for all the remaining data and make index entry
          }else{
          
            // calculate suitable length for new part -> multiple of VER_BLOCK_SIZE;
            rem_length += (VER_BLOCK_SIZE - (rem_length % VER_BLOCK_SIZE));

            // check if device has enough free space and index position is available
            if(checkNextPosAndSpace(next_free_index, rem_length) < 0){
              pthread_mutex_unlock(&lock);
              return -1;
            }

            // create new index entry
            ind[next_free_index].status = 0x80;
            ind[next_free_index].type = ind[fileID].type;
            ind[next_free_index].begin = append;
            ind[next_free_index].part_length = rem_length;
            ind[next_free_index].nextBlock = next_free_index;
            ind[next_free_index].firstBlock = fileID;
            ind[next_free_index].cur_length;
            strncpy(ind[next_free_index].logName, ind[fileID].logName, 37);
            ind[next_free_index].logName[37] = '\0';

            // write new index page to disk
            if(writeBackIndex(next_free_index) == -1){
              pthread_mutex_unlock(&lock);
              return -1;
            }

            // set pointer to new part
            ind[cur_ind].nextBlock = next_free_index;

            // write changed index back to disk
            if(writeBackIndex(cur_ind) == -1){
              pthread_mutex_unlock(&lock);
              return -1;
            }

            end_index_entry = next_free_index;
            length_at_end = in_file_pos;

            // change values
            append += rem_length;
            next_free_index++;

            // remaining length is 0
            rem_length = 0;
          }
        }
      // start position for write is not in current part
      }else{
        // case 1: end position in this part - nothing to do
        if(aligned_end_pos < in_file_pos + ind[cur_ind].part_length){
          length_at_end = in_file_pos;
          end_index_entry = cur_ind;
          // no bytes remaining
          rem_length = 0;
        // case 2: end position is not in this part
        }else{
          // if aligned_start_pos is already surpassed, remaining length must be calulated
          if(in_file_pos >= aligned_start_pos){
            rem_length -= ind[cur_ind].part_length;
          }
          in_file_pos += ind[cur_ind].part_length;

          // case 1: there exists another part - scan this part
          if(ind[cur_ind].nextBlock != cur_ind){
            cur_ind = ind[cur_ind].nextBlock;
          // case 2: there is no other part - a new part must be allocated
          }else{
            // calculate suitable length for new part -> multiple of VER_BLOCK_SIZE
            // if start-position is not surpassed, offset has to be added to remaining length
            if(in_file_pos <= aligned_start_pos){
              rem_length += (aligned_start_pos - in_file_pos);
            }
            rem_length += (VER_BLOCK_SIZE - (rem_length % VER_BLOCK_SIZE));

            // check if device has enough free space and index position is available
            if(checkNextPosAndSpace(next_free_index, rem_length) < 0){
              pthread_mutex_unlock(&lock);
              return -1;
            }

            // create new index entry
            ind[next_free_index].status = 0x80;
            ind[next_free_index].type = ind[fileID].type;
            ind[next_free_index].begin = append;
            ind[next_free_index].part_length = rem_length;
            ind[next_free_index].nextBlock = next_free_index;
            ind[next_free_index].firstBlock = fileID;
            ind[next_free_index].cur_length = 0;
            strncpy(ind[next_free_index].logName, ind[fileID].logName, 37);
            ind[next_free_index].logName[37] = '\0';

            // write new index page to disk
            if(writeBackIndex(next_free_index) == -1){
              pthread_mutex_unlock(&lock);
              return -1;
            }

            // set pointer to new part
            ind[cur_ind].nextBlock = next_free_index;

            // write changed index page to disk
            if(writeBackIndex(cur_ind) == -1){
              pthread_mutex_unlock(&lock);
              return -1;
            }

            // if start-position is not surpassed, it is in new allocated block;
            if(in_file_pos <= aligned_start_pos){
              start_index_entry = next_free_index;
              length_at_begin = in_file_pos;
            }
            end_index_entry = next_free_index;
            length_at_end = in_file_pos;

            // change values
            append += rem_length;
            next_free_index++;

            // there are no remaining bytes
            rem_length = 0;
          }
        }
      }
    }

    pthread_mutex_unlock(&lock);
    
    fflush(stdout);

    // the needed space on device is allocated - now we can read/write the data without locks

    //fill buffer with data
    if(off_start){
      pread(device, (void*) w_buf, BLOCKSIZE, aligned_start_pos - length_at_begin + ind[start_index_entry].begin);
    }
    // if it is only 1 byte t next blocksize-aligned position, then nothing to do
    if(off_end != 1){
      pread(device, (void*) (w_buf + aligned_length - BLOCKSIZE), BLOCKSIZE, ind[end_index_entry].begin + ((aligned_end_pos - length_at_end) - BLOCKSIZE + 1));
    }
    // read the bytes from data that should be written - see in write() for reasons why using Set/GetByteArrayRegion
    (*env)->GetByteArrayRegion(env, data, offset, length, w_buf + off_start);

    // loop that writes buffer to disk
    rem_length = aligned_length;
    cur_ind = start_index_entry;
    int ret = 0;
    uint32_t start_disp = aligned_start_pos - length_at_begin;
    uint32_t end_disp = 0;

    while(rem_length > 0){
      if(cur_ind == end_index_entry){
        end_disp = ind[cur_ind].part_length - start_disp - rem_length;
      }
      uint32_t write_length = ind[cur_ind].part_length - start_disp - end_disp;

      if(pwrite(device, (void*) (w_buf + (aligned_length - rem_length)), write_length, ind[cur_ind].begin + start_disp) < 0){
        return -1;
      }

      if(cur_ind == start_index_entry){
        start_disp = 0;
      }
      cur_ind = ind[cur_ind].nextBlock;
      rem_length -= write_length;
    }
    
    // update length-attribute of the file
    // this operation does not return the real length of the file (since it is written in fixed-size-parts)
    // but it is sufficient for our use, because DXRAM only appends to Versionlogs or writes them from beginning on
    pthread_mutex_lock(&lock);
    ind[fileID].cur_length = write_pos + length;
    // write changed index page to disk
    if(writeBackIndex(fileID) == -1){
      pthread_mutex_unlock(&lock);
      return -1;
    }
    pthread_mutex_unlock(&lock);


    // free the buffer if it was new allocated
    if(buf_created)
    {
      free(w_buf);
    }
    return (jint)ret;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    dread
 * reads from a dynamically growing file into data-buffer at given position
 * fileID:    the file descriptor
 * data:      reference to the byte buffer that should receive the data
 * offset:    start-offset in data-buffer
 * length:    number of bytes to read
 * read_pos:  read-offset in file
 * w_ptr:     address of preallocated readbuffer
 * w_length:  length of preallocated readbuffer
 * return:    0 on success or -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_dread
  (JNIEnv *env, jclass clazz, jint fileID, jbyteArray data, jint offset, jint length, jlong read_pos, jlong r_ptr, jint r_length)
  {
    // pointer to readbuffer
    char* r_buf;
    // flag to check if buffer was created
    char buf_created = 0;

    // get current position in file where read-access starts
    long start_pos = read_pos;
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
    
    uint32_t start_index_entry = 0;       // index of part contsining aligned_start_pos
    uint32_t end_index_entry = 0;         // index of part containing aligned_end_pos
    uint32_t length_at_begin = 0;         // in-file-offset of the beginning of the start_index-part
    uint32_t cur_ind = fileID;            // index to scan
    uint32_t rem_length = aligned_length;   // length that is not read from a part yet
    uint32_t in_file_pos = 0;             // current position in file
    
    // find start index for aligned_start_pos
    while(aligned_start_pos >= in_file_pos + ind[cur_ind].part_length){
      in_file_pos = in_file_pos + ind[cur_ind].part_length;
      // if there is no other block -> no read possible
      if(ind[cur_ind].nextBlock == cur_ind){
        return -1;
      }
      cur_ind = ind[cur_ind].nextBlock;
    }
    // aligned_start_pos is in part with index cur_ind
    start_index_entry = cur_ind;
    length_at_begin = in_file_pos;
    
    // find index for aligned_end_pos
    while(aligned_end_pos >= in_file_pos + ind[cur_ind].part_length){
      in_file_pos = in_file_pos + ind[cur_ind].part_length;
      // if there is no other block -> no read possible
      if(ind[cur_ind].nextBlock == cur_ind){
        return -1;
      }
      cur_ind = ind[cur_ind].nextBlock;
    }
    // alinged_end_pos is in part with index cur_ind
    end_index_entry = cur_ind;
    
    // loop that reads to buffer
    cur_ind = start_index_entry;
    int ret = 0;
    uint32_t start_disp = aligned_start_pos - length_at_begin;
    uint32_t end_disp = 0;
    uint32_t read_length = 0;

    while(rem_length > 0){
      if(cur_ind == end_index_entry){
        end_disp = ind[cur_ind].part_length - start_disp - rem_length;
      }
      read_length = ind[cur_ind].part_length - start_disp - end_disp;

      if(pread(device, (void*) (r_buf + (aligned_length - rem_length)), read_length, ind[cur_ind].begin + start_disp) < 0){
        return -1;
      }

      if(cur_ind == start_index_entry){
        start_disp = 0;
      }
      cur_ind = ind[cur_ind].nextBlock;
      rem_length -= read_length;
    }

    // set the bytes in buffer - see in write() for reasons why using Set/GetByteArrayRegion
    (*env)->SetByteArrayRegion(env, data, offset, length, r_buf + off_start);

    // free allocated buffer
    if (buf_created) {
      free(r_buf);
    }

    return (jint)ret;
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    length
 * returns the preallocated length of the file, i.e. the sum of the preallocated sizes of the blocks belonging to the file
 * warning(!): it does not the return the point until which the file is filled up with data,
 * as it is needed for writing into VersionLogs (use dlength for this)
 * fileID:	file descriptor
 * return: 	the allocated length of file
 */
JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_length
  (JNIEnv *env, jclass clazz, jint fileID)
  {
    long ret = ind[fileID].part_length;
    // scan for next block
    uint32_t cur_ind = fileID;
    // Scan all blocks of the file and sum up the preallocated blocksizes
    while(ind[cur_ind].nextBlock != cur_ind){
      cur_ind = ind[cur_ind].nextBlock;
      ret += ind[cur_ind].part_length;
    }	
    return ret;
  }
  
/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    dlength
 * returns the last write endposition in a dynamically growing file
 * for VersionLogs this is the point, until which the file is filled up with data -
 * it shold be used to determine the 'real' length of a file that is dynamically growing
 * and where write-access only appends or starts new from 0
 * fileID:	file descriptor
 * return: 	last write position (length) of file
 */
JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_dlength
  (JNIEnv *env, jclass clazz, jint fileID)
  {
    long ret = ind[fileID].cur_length;
    return (jlong)ret;
  }
  
/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    setDFileLength
 * sets the write position (length) of a dynamically growing file - use in VersionLogs
 * fileID:    the file descriptor
 * length:    new length of file
 * return:    0 on success, -1 on error
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_setDFileLength
  (JNIEnv *env, jclass clazz, jint fileID, jlong length)
  {
    pthread_mutex_lock(&lock);
    ind[fileID].cur_length = (uint32_t)length;
    // write changed index page to disk
    if(writeBackIndex(fileID) == -1){
      pthread_mutex_unlock(&lock);
      return -1;
    }
    pthread_mutex_unlock(&lock);
    return 0;
  }
  
/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    deleteLog
 * deletes an exisiting log from index, so that space could be used for another log
 * fileID:  the file-deskriptor
 * return:  0 on success, -1 on error (log was opened or writing back index failed)
 */
JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_deleteLog
  (JNIEnv *env, jclass clazz, jint fileID)
  {
    pthread_mutex_lock(&lock);
    // Check if log is opened and reutrn error
    if(((ind[fileID].status) & sel_bit_second) != 0){
      pthread_mutex_unlock(&lock);
      return -1;
    }
    // set third bit for deleted and delete used-bit
    ind[fileID].status = ind[fileID].status | set_bit_third;
    ind[fileID].status = ind[fileID].status & null_bit_first;
    // write changed index back
    int ret = writeBackIndex(fileID);
    pthread_mutex_unlock(&lock);
    
    return ret;
  }
  
/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    getFileList
 * returns a String with all names of the logs, seperated by \n
 * return:  String with all lognames
 */
JNIEXPORT jstring JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_getFileList
  (JNIEnv *env, jclass clazz)
  {
    // acquire lock becuase index must not be chnaged during this
    pthread_mutex_lock(&lock);
    
    // first: calculate length of resulting string
    uint32_t string_length = 0;
    uint32_t i = 0;
    // loop while entry is in use
    while(((ind[i].status) & sel_bit_first) != 0x00){
      if(ind[i].firstBlock == i){
        // add length of string
        string_length += strlen(ind[i].logName);
        // add \n character
        string_length++;
      }
      i++;
    }
    // add 0-Terminator
    string_length++;
    
    // now create resulting String -> each entry has 37 characters + \n, in addition \0 for whole string
    char *res;
    if((res = malloc(string_length)) == NULL){
      pthread_mutex_unlock(&lock);
      return (*env)->NewStringUTF(env, "Failure");
    }
    
    i = 0;
    uint32_t cur_pos = 0;
    while(((ind[i].status) & sel_bit_first) != 0x00){
      if(ind[i].firstBlock == i){
        // String is terminated in any case
        strcpy((res + cur_pos), ind[i].logName);
        // set linebreak
        res[cur_pos + strlen(ind[i].logName)] = '\n';
        // next position in array
        cur_pos += (strlen(ind[i].logName) + 1);
      }
      i++;
    }
    // set 0-Terminator
    res[string_length-1] = '\0';
    
    pthread_mutex_unlock(&lock);
    
    return (*env)->NewStringUTF(env, res);
    
  }

/*
 * Class:     de_hhu_bsinfo_utils_JNIFileRaw
 * Method:    print_index_for_test
 * prints all logs in index together with their indexnumber, size and name. Only for tests.
 */
JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNIFileRaw_print_1index_1for_1test
  (JNIEnv *env, jclass clazz)
  {
    printf("(List of logs or segments in index)\n");

    uint32_t i = 0;
    for(i=0; i < index_length; i++){
      // check if the first bit of status is set
      if(((ind[i].status) & sel_bit_first) != 0x00){
        printf("Log index %u and logname %s with size %u MB. It begins at position %lu \n", i, ind[i].logName, ind[i].part_length / (1024*1024), ind[i].begin);
      }else{
        break;
      }
    }
  }