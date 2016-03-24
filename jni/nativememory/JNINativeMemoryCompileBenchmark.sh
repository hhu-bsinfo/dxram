#!/bin/bash

gcc -O1 -shared -fpic -o libJNINativeMemory_gcc_o1.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c
gcc -O2 -shared -fpic -o libJNINativeMemory_gcc_o2.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c
gcc -O3 -shared -fpic -o libJNINativeMemory_gcc_o3.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c

clang -O1 -shared -fpic -o libJNINativeMemory_clang_o1.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c
clang -O2 -shared -fpic -o libJNINativeMemory_clang_o2.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c
clang -O3 -shared -fpic -o libJNINativeMemory_clang_o3.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c

/opt/intel/bin/icc -O1 -shared -fpic -o libJNINativeMemory_icc_o1.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c
/opt/intel/bin/icc -O2 -shared -fpic -o libJNINativeMemory_icc_o2.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c
/opt/intel/bin/icc -O3 -shared -fpic -o libJNINativeMemory_icc_o3.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNINativeMemory.c
