# Ibnet: InfiniBand core subsystem and Ibdxnet interface for DXRAM

This project uses the ibverbs library to create a core InfiniBand subsystem
in C++ as well as the JNI library Ibdxnet which is used by the 
distributed in-memory key-value store [DXRAM](https://github.com/hhu-bsinfo/dxram)
to communicate between nodes using InfiniBand.

However, the core of the library can also be used with other (C++) applications.

# Setup
You need hardware that supports the ibverbs library. How to setup your hardware
is not part of this project. However, I took a few notes for myself that might
help you [here](doc/Setup.md).

# Build instructions
This project supports Linux, only, and uses the ibverbs library to access 
InfiniBand hardware. CMake scripts are used to generate build scripts 
(e.g. makefile).

* Go to the ibdxnet folder
* *mkdir build*
* *cd build*
* *cmake ..*
* *make*

# Run instructions
The core is independent of DXRAM and includes a few tests that can be started 
without having to install DXRAM.

To run Ibdxnet with DXRAM, follow the instructions in the respective readme of
the DXRAM repository.