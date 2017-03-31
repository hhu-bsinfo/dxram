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

/* Some structures, definitions and Macros with explanation
 * use for JNIFileRAW.c
 *
 * Created for DXRAM by Christian Gesse, 2016 */

#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>


/* The following macros are parameters that can be tuned for different devices */
// predefined Blocksize of hardware device (4096 for SSD in most cases) 
// should be FlashPageSize from DXRAM
#define BLOCKSIZE 4096
// use predefined size for parts of VersionLogs (16MB is good for 64 byte chunks)
// optimize for different sizes of VersionLogs
#define VER_BLOCK_SIZE (16*1024*1024)
// size of secondary-logs - for calculation of index-length we need log-size for secondary-logs
// check size in config of DXRAM!
#define SEC_LOGSIZE (512*1024*1024)
// this factor is needed to calculate the size of the preallocated fix index - standard is 6
#define INDEX_FACTOR 6

/* Bitmasks to switch several bits on/off in char */
// set first bit - use with OR
const char set_bit_first = 0x80;
// set second bit - use with OR
const char set_bit_second = 0x40;
// set third bit - use with OR
const char set_bit_third = 0x20;
// delete first bit - use with AND
const char null_bit_first = 0x7F;
// delete second bit - use with AND
const char null_bit_second = 0xBF;
// delete third bit - use with AND
const char null_bit_third = 0xDF;
// select only first bit - use with AND
const char sel_bit_first = 0x80;
// select only second bit - use with AND
const char sel_bit_second = 0x40;
// select only third bit - use with AND
const char sel_bit_third = 0x20;


// indicators for different logtypes
const char prim_indicator[] = "prim.log"; // primary
const char sec_indicator[] = ".log";      // secondary
const char ver_indicator[] = ".ver";      // version

/* Store metadata for logs in one index at the beginning of the raw-device.
  To guarantee growing version-logs we give the possibility to chain a couple of
  index-entries with next-pointer. Each index-entry has -similar to an array -
  an index-number, beginning with 0 for the first entry for the primary log.*/

// structure of a header for the rawdevice
struct __attribute__((__packed__)) device_header{
  char begin[12];               // begin number of logging-device; for recognition if device was used before -> value should be "DXRAMdevice"
  uint64_t length;              // length of partition in bytes
  uint32_t blocksize;           // used BLOCKSIZE
  uint32_t index;               // start of index-data
  uint32_t index_length;        // number of possible entries in index
  uint32_t data;                // start of log-data
  uint32_t entry_size;          // size of 1 index entry
  uint32_t entries_per_block;   // number of index entries per block
};

typedef struct device_header device_header_t;




// structur for index entry in log-index
// since it is 64Bytes, exactly 64 entries can be stored in one block of size 4096
// this enables us to use an array of entries as read/write buffer
struct __attribute__((__packed__)) index_entry{
  char      status;       // status bits of the log, in right order: used, opened, deleted (last bits only set for the first block, used for every block)
  char      logName[38];  // filename of the log, maximum of 37 characters
  char      type;         // type of log: p = primary, s = secondary, v = version
  uint64_t  begin;        // begin address of this log (or part of log) in raw-device
  uint32_t  part_length;  // length of the part or of the entire log (in case of static size); max at 4GB per part
  uint32_t  nextBlock;    // index-number of next part of the log (only needed for dynamically growing logs) - initial value is index number of starblock
  uint32_t  firstBlock;   // index number of the first part of the log (only needed for dynamically growing logs)
  uint32_t  cur_length;   // the current written length-position of the log - only needed and set for version log and only set in first part with fileID
};

typedef struct index_entry index_entry_t;
