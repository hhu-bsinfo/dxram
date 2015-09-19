Linux:

  JNILock: Set absolute path to libJNILock.so properly in dxram.config
  
  JNIconsole: Set absolute path to libJNIconsole.so properly in dxram.config
	      Error:
	      1) Install libreadline-dev
	      2) Copy libreadline.so to dxram/jni/
	      3) Compile JNIconsole.c: gcc -c JNIconsole.c -o JNIconsole.o -I/usr/lib/jvm/java-8-oracle/include -I/usr/lib/jvm/java-8-oracle/include/linux/
	      4) Link JNIconsole.o to libJNIconsole.so: g++ -shared JNIconsole.o -o libJNIconsole.so libreadline.so -ltermcap
	      5) Set absolute path to libJNIconsole.so properly in dxram.config
	      
	      
Mac OS X:

  JNILock: Set absolute path to libJNILock.dylib properly in dxram.config
  
  JNIconsole: Set absolute path to libJNIconsole.dylib properly in dxram.config
	      Error:
	      1) Manually install readline
	      2) Copy libreadline.a to dxram/jni/
	      3) Compile JNIconsole.c: gcc -c JNIconsole.c -o JNIconsole.o -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include
	      4) Link JNIconsole.o to libJNIconsole.dylib: g++ -dynamiclib -undefined suppress -flat_namespace *.o -o libJNIconsole.dylib libreadline.a -ltermcap
	      5) Set absolute path to libJNIconsole.dylib properly in dxram.config