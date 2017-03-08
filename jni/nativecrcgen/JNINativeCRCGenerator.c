/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
// gcc -O2 -shared -fpic -o libJNINativeCRCGenerator.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeCRCGenerator.c  -lz -msse4.2
// mac:
// gcc -O2 -shared -fpic -o libJNINativeCRCGenerator.dylib -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include/darwin JNINativeCRCGenerator.c  -lz -msse4.2

#include <jni.h>
#include <stdint.h>
#include <zlib.h>
#include <x86intrin.h>


/*uint32_t sse42_crc32(uint32_t p_crc, const uint8_t* p_data, uint32_t p_offset, uint32_t p_length) {
  uint32_t i = 0;
  for (i = 0; i < p_length; i++) {
    p_crc = _mm_crc32_u8(p_crc, p_data[i + p_offset]);
  }
  //0x11EDC6F41

  return p_crc;
}*/

uint32_t sse42_crc32(uint32_t p_crc, const uint8_t* p_data, uint32_t p_offset, uint32_t p_length) {
  uint32_t i = 0;
  while (i + 8 <= p_length) {
    p_crc = _mm_crc32_u64(p_crc, *((uint64_t *) &p_data[i + p_offset]));
    i += 8;
  }
  
  if (i + 4 <= p_length) {
    p_crc = _mm_crc32_u32(p_crc, *((uint32_t *) &p_data[i + p_offset]));
    i += 4;
  }
  
  if (i + 2 <= p_length) {
    p_crc = _mm_crc32_u16(p_crc, *((uint16_t *) &p_data[i + p_offset]));
    i += 2;
  }
  
  if (i + 1 <= p_length) {
    p_crc = _mm_crc32_u8(p_crc, p_data[i + p_offset]);
    i++;
  }

  return p_crc;
}

uint32_t default_crc32(uint32_t p_crc, const uint8_t* p_data, uint32_t p_offset, uint32_t p_length) {
  return crc32(p_crc, p_data + p_offset, p_length);
}


/* this will be called at load time to decide which function really use */
/* sse42_crc32 if SSE 4.2 is supported */
/* default_crc32 if not */
static void * resolve_crc32(void) {
  __builtin_cpu_init();
  if (__builtin_cpu_supports("sse4.2")) return sse42_crc32;

  return default_crc32;
}

/* crc32_sse_long() implementation will be resolved at load time to either */
/* sse42_crc32_long() or default_crc32() */
uint32_t crc32_sse(uint32_t p_crc, const uint8_t* p_data, uint32_t p_offset, uint32_t p_length) __attribute__ ((ifunc ("resolve_crc32")));


JNIEXPORT jint JNICALL Java_de_hhu_bsinfo_utils_JNINativeCRCGenerator_hash(JNIEnv *p_env, jclass p_class, jint p_checksum, jbyteArray p_data, jint p_offset, jint p_length) {
	jint ret;
	
	char* data = (*p_env)->GetPrimitiveArrayCritical(p_env, p_data, 0);
	if (data) {
		ret = crc32_sse(p_checksum, data, p_offset, p_length);
		(*p_env)->ReleasePrimitiveArrayCritical(p_env, p_data, data, 0);
	}
	
	return ret;
}