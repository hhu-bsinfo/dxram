# JVM crashing on JNI calls

If your Java application is crashing (either right at the start or after a 
random amount of time) when trying to attach the current thread to the JVM
similar to:
```commandline
Stack trace (most recent call last) in thread 18190:
#23   Object "", at 0xffffffffffffffff, in 
#22   Source "../sysdeps/unix/sysv/linux/x86_64/clone.S", line 109, in __clone [0x7f6d99e0282c]
#21   Source "/build/glibc-t3gR2i/glibc-2.23/nptl/pthread_create.c", line 333, in start_thread [0x7f6d996c86b9]
#20   Object "/usr/lib/x86_64-linux-gnu/libstdc++.so.6.0.21", at 0x7f6d984bac7f, in 
#19   Source "/usr/include/c++/5/thread", line 115, in _M_run [0x7f6d5e928109]
        112: 	{ }
        113: 
        114: 	void
      > 115: 	_M_run() { _M_func(); }
        116:       };
        117: 
        118:   private:
#18   Source "/usr/include/c++/5/functional", line 1520, in operator() [0x7f6d5e9283b9]
       1517:       operator()()
       1518:       {
       1519:         typedef typename _Build_index_tuple<sizeof...(_Args)>::__type _Indices;
      >1520:         return _M_invoke(_Indices());
       1521:       }
       1522: 
       1523:     private:
#17   Source "/usr/include/c++/5/functional", line 1531, in _M_invoke<0ul> [0x7f6d5e92864c]
       1528: 	  // std::bind always forwards bound arguments as lvalues,
       1529: 	  // but this type can call functions which only accept rvalues.
       1530:           return std::forward<_Callable>(std::get<0>(_M_bound))(
      >1531:               std::forward<_Args>(std::get<_Indices+1>(_M_bound))...);
       1532:         }
       1533: 
       1534:       std::tuple<_Callable, _Args...> _M_bound;
#16   Source "/usr/include/c++/5/functional", line 600, in operator()<, void> [0x7f6d5e9286b8]
        597:                           _CheckArgs<_Pack<_Args...>>>>
        598: 	result_type
        599: 	operator()(_Class* __object, _Args&&... __args) const
      > 600: 	{ return (__object->*_M_pmf)(std::forward<_Args>(__args)...); }
        601: 
        602:       // Handle smart pointers, references and pointers to derived
        603:       template<typename _Tp, typename... _Args, typename _Req
#15   Source "/home/nothaas/infiniband/ibnet/cmake/../src/ibnet/sys/Thread.h", line 94, in __Run [0x7f6d5e917dc0]
         91:     {
         92:         try {
         93:             IBNET_LOG_INFO("Started thread {}", m_name);
      >  94:             _Run();
         95:             IBNET_LOG_INFO("Finished thread {}", m_name);
         96:         } catch (Exception& e) {
         97:             e.PrintStackTrace();
#14   Source "/home/nothaas/infiniband/ibnet/cmake/../src/ibnet/sys/ThreadLoop.h", line 71, in _Run [0x7f6d5e92c701]
         68:         _BeforeRunLoop();
         69: 
         70:         while (m_run) {
      >  71:             _RunLoop();
         72:         }
         73: 
         74:         _AfterRunLoop();
#13   Source "/home/nothaas/infiniband/ibnet/src/ibnet/msg/RecvThread.cpp", line 114, in _RunLoop [0x7f6d5e92a4b3]
        111:         return;
        112:     }
        113: 
      > 114:     __ProcessBuffers();
        115: }
        116: 
        117: void RecvThread::_AfterRunLoop(void)
#12   Source "/home/nothaas/infiniband/ibnet/src/ibnet/msg/RecvThread.cpp", line 243, in __ProcessBuffers [0x7f6d5e92af46]
        240:         m_timers[7].Enter();
        241: 
        242:         m_messageHandler->HandleMessage(sourceNode,
      > 243:             poolEntry.m_mem->GetAddress(), recvLength);
        244: 
        245:         m_timers[7].Exit();
        246:     }
#11   Object "/home/nothaas/dxram_ib/jni/libJNIIbnet.so", at 0x7f6d5e908c43, in ibnet::jni::MessageHandler::HandleMessage(unsigned short, void*, unsigned int)
#10   Object "/home/nothaas/dxram_ib/jni/libJNIIbnet.so", at 0x7f6d5e9086d1, in ibnet::jni::Callbacks::HandleReceive(unsigned short, void*, unsigned int)
#9    Object "/home/nothaas/dxram_ib/jni/libJNIIbnet.so", at 0x7f6d5e9088aa, in ibnet::jni::Callbacks::__GetEnv()
#8    Object "/home/nothaas/dxram_ib/jni/libJNIIbnet.so", at 0x7f6d5e9050f1, in JavaVM_::AttachCurrentThread(void**, void*)
#7  | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/prims/jni.cpp", line 5490, in attach_current_thread
      Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/prims/jni.cpp", line 5498, in jni_AttachCurrentThread [0x7f6d98df8ebc]
#6    Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/prims/jni.cpp", line 5422, in attach_current_thread [0x7f6d98df8b9b]
#5  | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/thread.cpp", line 1153, in allocate_instance_handle
      Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/oops/instanceKlass.hpp", line 761, in allocate_threadObj [0x7f6d9918985a]
#4  | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/oops/instanceKlass.cpp", line 1150, in obj_allocate
      Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/gc_interface/collectedHeap.inline.hpp", line 203, in allocate_instance [0x7f6d98da3b13]
#3  | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/gc_interface/collectedHeap.inline.hpp", line 175, in common_mem_allocate_noinit
      Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/gc_interface/collectedHeap.inline.hpp", line 135, in common_mem_allocate_init [0x7f6d98da8da1]
#2    Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/gc_implementation/parallelScavenge/parallelScavengeHeap.cpp", line 324, in mem_allocate [0x7f6d9905dbe7]
#1    Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/vmThread.cpp", line 642, in execute [0x7f6d991f187b]
#0  | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/mutex.cpp", line 1117, in ThreadBlockInVM
    | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/interfaceSupport.hpp", line 311, in trans_and_fence
    | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/interfaceSupport.hpp", line 232, in transition_and_fence
    | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/interfaceSupport.hpp", line 179, in serialize_memory
    | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/os/linux/vm/interfaceSupport_linux.hpp", line 31, in write_memory_serialize_page
      Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/os.hpp", line 419, in wait [0x7f6d98ff98ba]
Segmentation fault (Invalid permissions for mapped object [0x7f6d9a4f5100])
Segmentation fault
```

or:
```commandline
Stack trace (most recent call last) in thread 24476:
#16   Object "", at 0xffffffffffffffff, in 
#15   Source "../sysdeps/unix/sysv/linux/x86_64/clone.S", line 109, in __clone [0x7f392d5a282c]
#14   Source "/build/glibc-t3gR2i/glibc-2.23/nptl/pthread_create.c", line 333, in start_thread [0x7f392ce686b9]
#13   Object "/usr/lib/x86_64-linux-gnu/libstdc++.so.6.0.21", at 0x7f392bc5ac7f, in 
#12   Source "/usr/include/c++/5/thread", line 115, in _M_run [0x7f390a14df3b]
        112: 	{ }
        113: 
        114: 	void
      > 115: 	_M_run() { _M_func(); }
        116:       };
        117: 
        118:   private:
#11   Source "/usr/include/c++/5/functional", line 1520, in operator() [0x7f390a14e1eb]
       1517:       operator()()
       1518:       {
       1519:         typedef typename _Build_index_tuple<sizeof...(_Args)>::__type _Indices;
      >1520:         return _M_invoke(_Indices());
       1521:       }
       1522: 
       1523:     private:
#10   Source "/usr/include/c++/5/functional", line 1531, in _M_invoke<0ul> [0x7f390a14e47e]
       1528: 	  // std::bind always forwards bound arguments as lvalues,
       1529: 	  // but this type can call functions which only accept rvalues.
       1530:           return std::forward<_Callable>(std::get<0>(_M_bound))(
      >1531:               std::forward<_Args>(std::get<_Indices+1>(_M_bound))...);
       1532:         }
       1533: 
       1534:       std::tuple<_Callable, _Args...> _M_bound;
#9    Source "/usr/include/c++/5/functional", line 600, in operator()<, void> [0x7f390a14e4ea]
        597:                           _CheckArgs<_Pack<_Args...>>>>
        598: 	result_type
        599: 	operator()(_Class* __object, _Args&&... __args) const
      > 600: 	{ return (__object->*_M_pmf)(std::forward<_Args>(__args)...); }
        601: 
        602:       // Handle smart pointers, references and pointers to derived
        603:       template<typename _Tp, typename... _Args, typename _Req
#8    Source "/home/nothaas/infiniband/ibnet/cmake/../src/ibnet/sys/Thread.h", line 94, in __Run [0x7f390a13dbf2]
         91:     {
         92:         try {
         93:             IBNET_LOG_INFO("Started thread {}", m_name);
      >  94:             _Run();
         95:             IBNET_LOG_INFO("Finished thread {}", m_name);
         96:         } catch (Exception& e) {
         97:             e.PrintStackTrace();
#7    Source "/home/nothaas/infiniband/ibnet/cmake/../src/ibnet/sys/ThreadLoop.h", line 71, in _Run [0x7f390a152533]
         68:         _BeforeRunLoop();
         69: 
         70:         while (m_run) {
      >  71:             _RunLoop();
         72:         }
         73: 
         74:         _AfterRunLoop();
#6    Source "/home/nothaas/infiniband/ibnet/src/ibnet/msg/RecvThread.cpp", line 114, in _RunLoop [0x7f390a1502e5]
        111:         return;
        112:     }
        113: 
      > 114:     __ProcessBuffers();
        115: }
        116: 
        117: void RecvThread::_AfterRunLoop(void)
#5    Source "/home/nothaas/infiniband/ibnet/src/ibnet/msg/RecvThread.cpp", line 243, in __ProcessBuffers [0x7f390a150d78]
        240:         m_timers[7].Enter();
        241: 
        242:         m_messageHandler->HandleMessage(sourceNode,
      > 243:             poolEntry.m_mem->GetAddress(), recvLength);
        244: 
        245:         m_timers[7].Exit();
        246:     }
#4    Object "/home/nothaas/dxram_ib/jni/libJNIIbnet.so", at 0x7f390a12ea75, in ibnet::jni::MessageHandler::HandleMessage(unsigned short, void*, unsigned int)
#3    Object "/home/nothaas/dxram_ib/jni/libJNIIbnet.so", at 0x7f390a12e575, in ibnet::jni::Callbacks::HandleReceive(unsigned short, void*, unsigned int)
#2    Object "/home/nothaas/dxram_ib/jni/libJNIIbnet.so", at 0x7f390a12adf6, in JNIEnv_::CallObjectMethod(_jobject*, _jmethodID*, ...)
#1  | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/prims/jni.cpp", line 1729, in ThreadInVMfromNative
    | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/interfaceSupport.hpp", line 278, in trans_from_native
      Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/interfaceSupport.hpp", line 231, in jni_CallObjectMethodV [0x7f392c5a1a9c]
#0  | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/interfaceSupport.hpp", line 212, in serialize_memory
    | Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/os/linux/vm/interfaceSupport_linux.hpp", line 31, in write_memory_serialize_page
      Source "/build/openjdk-8-VTMhfL/openjdk-8-8u131-b11/src/hotspot/src/share/vm/runtime/os.hpp", line 419, in transition_from_native [0x7f392c593ad8]
Segmentation fault (Invalid permissions for mapped object [0x7f392dc95100])
```

...you have to use the command line argument *-XX:+UseMembar* when running your 
Java application.

## What happened?
I don't know for sure but judging by different bug reports in the past 
(see further references) it could be a bug in the JVM.

When calling *AttachCurrentThread* to attach the current non JVM thread to the 
JVM in order to call back into the Java context, the thread has to undergo
a state transition which is shared with other JVM threads (a comment mentions 
the GC thread explicitly). 

There are two methods which can be selected by switching the *UseMembar* 
parameter. The default method (in openjdk 8u131) uses memory serialize page 
(UseMembar=false). The other option uses memory barrier instructions 
(UseMembar=true).

For some reason, the default option crashes due to synchronization issues but
the memory barrier method works fine.

## Further references
* https://stackoverflow.com/questions/1120088/what-is-javas-xxusemembar-parameter
* http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6546278
* http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6822370

# Every microsecond counts, slow operations with JNI

A list of don'ts that slow down things and kill performance (with InfiniBand):
* Direct ByteBuffer allocation: Pool them if possible
* Using *GetDirectBufferAddress* or *GetDirectBufferCapacity*: Use field access
instead
* Exception checking and detaching thread when leaving JVM environment: If you
don't create and destroy threads and the same threads are entering the JVM
environment over and over, just keep them attached