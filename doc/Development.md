# Development with DXRAM

This document describes the different interfaces and methods that are provided by DXRAM and the build environment for
developing core features of DXRAM or external applications/algorithms running on DXRAM.

## Simple Unit-Tests

Simple and typically known unit-tests are developed using jUnit and test basic functionality of classes that are used
by core modules of DXRAM. Naturally, this is limited to a small subset and cannot test the DXRAM system as a whole
or even single services of it. This situation is covered by the DXRAM specific extended unit-tests (see next section).

## Extended Unit-Tests

DXRAM's extended unit-tests (extTest) use a special Junit-runner that bootstraps one or multiple DXRAM instances on
localhost and provides a framework for running simple test code on each of the bootstrapped instances. This allows
testing fundamental functionality of a size limited distributed setup without requiring multiple physical nodes and
deployment.

Attention: This framework is very resource intensive as it runs multiple full DXRAM instances on a single machine. This
requires appropriate CPU and memory resources to be useful. A CPU with at least four physical cores and 8 GB or main
memory is definitely recommended.

DXRAM already comes with various extTests to test the services of DXRAM (see *src/extTest*). Use these as examples
to start implementing your own tests either for testing new features added to DXRAM or to prototype your
application/algorithm.

The extTests require distribution build output to run. You have to create a distribution build first and provide the
path to it in the configuration file *config.properties* in the package *de.hhu.bsinfo.dxram*.

The following is a minimal example that can be treated as "Hello World" to get started with writing your own extTest:
```
@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class HelloWorldExtTest {
    @TestInstance(runOnNodeIdx = 1)
    public void test(final DXRAM p_instance) {

    }
}
```

When you run this (either in your IDE or using the build system), the test framework creates and starts two instances
in the order described in the class annotation: one superpeer and one peer. Both are started with default configurations
and no additional tweaks. You can change selected settings like the storage size, by specifying additional parameters
on each node (see the DXRAMTestConfiguration class).

The method *test* is annotated to be detected as a test method the runner has to run. The parameter *runOnNodeIdx*
specifies which node *p_instance* is provided to this method on execution. In this example, the instance of the peer
specified in the class-annotation is provided. You can use that instance to access DXRAM's core services like on
DXApps, tasks or jobs.

You can also specifiy multiple test methods running with different instances or on multiple. For example:
```
@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class HelloWorldExtTest2 {
    @TestInstance(runOnNodeIdx = 1)
    public void test1(final DXRAM p_instance) {

    }

    @TestInstance(runOnNodeIdx = 2)
    public void test2(final DXRAM p_instance) {

    }
}
```

This example runs the method *test1* on the first peer and *test2* on the second peer. Please note, that both methods
are executed by dedicated threads in the runner backend and, thus, your code is executed concurrently to some degree
(trying to mimic the environment of a real distributed setup).

You can also specify multiple nodes to run the same test code on:
```
@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class HelloWorldExtTest2 {
    @TestInstance(runOnNodeIdx = 1, 2)
    public void test(final DXRAM p_instance) {

    }
}
```

In this example, the method *test* is executed on both peers, the first and second one, concurrently.

## DXRAM as a client Application

DXRAM can be embedded into and started from existing applications and be used as a pure client that is part of the
DXRAM overlay and utilize DXNet, DXRAM's high performance network stack. Furthermore, the client has access to (nearly)
all services offered by DXRAM.

We do not recommend using the DXRAM client to develop applications running on DXRAM. This is recommended for existing
applications that want to integrate into the DXRAM cluster overlay, e.g. benchmarks like the YCSB. Instead, consider
using more lightweight solutions like implementing your algorithm as a task/job or a more heavy-weight application
as a DXApp (see next sections).

## Tasks for DXRAM's MasterSlaveService

TODO

## Jobs for DXRAM's JobService

TODO

## DXApps

TODO
