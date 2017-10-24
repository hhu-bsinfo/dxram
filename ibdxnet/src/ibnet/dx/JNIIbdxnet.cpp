/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf,
 * Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

#include "JNIIbdxnet.h"

#include <memory>

#include <backwards/backward.hpp>

#include "ibnet/sys/Logger.h"
#include "ibnet/sys/ProfileTimer.hpp"
#include "ibnet/core/IbConnection.h"
#include "ibnet/core/IbNodeConfArgListReader.h"
#include "ibnet/core/IbTimeoutException.h"

#include "ConnectionCreator.h"
#include "ConnectionHandler.h"
#include "DebugThread.h"

static std::unique_ptr<backward::SignalHandling> g_signalHandler;

static std::shared_ptr<ibnet::dx::ConnectionHandler> g_connectionHandler;
static std::shared_ptr<ibnet::dx::RecvHandler> g_recvHandler;
static std::shared_ptr<ibnet::dx::SendHandler> g_sendHandler;

static std::shared_ptr<ibnet::core::IbDevice> g_device;
static std::shared_ptr<ibnet::core::IbProtDom> g_protDom;

static std::shared_ptr<ibnet::core::IbSharedRecvQueue> g_sharedRecvQueue;
static std::shared_ptr<ibnet::core::IbSharedRecvQueue>
    g_sharedFlowControlRecvQueue;
static std::shared_ptr<ibnet::core::IbCompQueue> g_sharedRecvCompQueue;
static std::shared_ptr<ibnet::core::IbCompQueue>
    g_sharedFlowControlRecvCompQueue;

static std::shared_ptr<ibnet::core::IbConnectionManager> g_connectionManager;

static std::shared_ptr<ibnet::dx::SendBuffers> g_sendBuffers;
static std::shared_ptr<ibnet::dx::RecvBufferPool> g_recvBufferPool;

static std::shared_ptr<ibnet::dx::RecvThread> g_recvThread;
static std::shared_ptr<ibnet::dx::SendThread> g_sendThread;

static std::unique_ptr<ibnet::dx::DebugThread> g_debugThread;

JNIEXPORT jboolean JNICALL Java_de_hhu_bsinfo_dxnet_ib_JNIIbdxnet_init(
        JNIEnv* p_env, jclass p_class, jshort p_ownNodeId, jint p_inBufferSize,
        jint p_outBufferSize, jlong p_recvPoolSizeBytes, jint p_maxRecvReqs,
        jint p_maxSendReqs, jint p_flowControlMaxRecvReqs,
        jint p_maxNumConnections, jint p_connectionCreationTimeoutMs,
        jobject p_sendHandler, jobject p_recvHandler,
        jobject p_connectionHandler, jboolean p_enableSignalHandler,
        jboolean p_enableDebugThread)
{
    // setup foundation
    if (p_enableSignalHandler) {
        g_signalHandler = std::make_unique<backward::SignalHandling>();
    }

    ibnet::sys::Logger::Setup();

    IBNET_LOG_DEBUG("Foundation setup done");

    // first, check if we are allowed to pin memory, otherwise we don't have
    // to continue. just check if we are root...
    if (getuid() != 0) {
        IBNET_LOG_WARN("Running IB subsystem as NON ROOT USER!!!");
    }

    // callbacks to java vm

    try {
        g_connectionHandler = std::make_shared<ibnet::dx::ConnectionHandler>(
            p_env, p_connectionHandler, g_recvThread);
        g_recvHandler = std::make_shared<ibnet::dx::RecvHandler>(p_env,
            p_recvHandler);
        g_sendHandler = std::make_shared<ibnet::dx::SendHandler>(p_env,
            p_sendHandler);
    } catch (...) {
        IBNET_LOG_ERROR("Setting up callbacks to java vm failed");
        return (jboolean) 0;
    }

    IBNET_LOG_DEBUG("Setting up java vm callbacks done");

    try {
        IBNET_LOG_INFO("Initializing infiniband backend...");

        g_device = std::make_shared<ibnet::core::IbDevice>();
        g_protDom = std::make_shared<ibnet::core::IbProtDom>(g_device,
            "jni_ibnet");

        IBNET_LOG_DEBUG("Protection domain:\n{}", *g_protDom);

        g_sharedRecvQueue = std::make_shared<ibnet::core::IbSharedRecvQueue>(
            g_protDom,
            p_maxRecvReqs);
        g_sharedFlowControlRecvQueue =
            std::make_shared<ibnet::core::IbSharedRecvQueue>(g_protDom,
                p_flowControlMaxRecvReqs);

        g_sharedRecvCompQueue = std::make_shared<ibnet::core::IbCompQueue>(
            g_device,
            p_maxRecvReqs);
        g_sharedFlowControlRecvCompQueue =
            std::make_shared<ibnet::core::IbCompQueue>(g_device,
                p_flowControlMaxRecvReqs);

        // add nodes later
        ibnet::core::IbNodeConf nodeConf;

        g_connectionManager = std::make_shared<ibnet::core::IbConnectionManager>(
            p_ownNodeId, nodeConf, 5731, p_connectionCreationTimeoutMs,
            p_maxNumConnections, g_device, g_protDom,
            std::make_unique<ibnet::dx::ConnectionCreator>(p_maxSendReqs,
                p_maxRecvReqs, p_flowControlMaxRecvReqs, g_sharedRecvQueue,
                g_sharedRecvCompQueue, g_sharedFlowControlRecvQueue,
                g_sharedFlowControlRecvCompQueue));

        g_connectionManager->SetNodeConnectedListener(g_connectionHandler.get());

        IBNET_LOG_INFO("Initializing buffer pools...");

        g_sendBuffers = std::make_shared<ibnet::dx::SendBuffers>(
            p_outBufferSize, p_maxNumConnections, g_protDom);
        g_recvBufferPool = std::make_shared<ibnet::dx::RecvBufferPool>(
            p_recvPoolSizeBytes, p_inBufferSize, p_flowControlMaxRecvReqs,
            g_protDom);

        IBNET_LOG_INFO("Initializing send and recv thread...");

        g_recvThread = std::make_shared<ibnet::dx::RecvThread>(
            g_connectionManager, g_sharedRecvCompQueue,
            g_sharedFlowControlRecvCompQueue, g_recvBufferPool,
            g_recvHandler);
        g_recvThread->Start();

        g_sendThread = std::make_shared<ibnet::dx::SendThread>(
            p_inBufferSize, g_sendBuffers, g_sendHandler,
            g_connectionManager);
        g_sendThread->Start();
    } catch (std::exception& e) {
        IBNET_LOG_PANIC("Initializing infiniband backend failed: {}", e.what());

        // no need to shutdown everything in order because this error can't
        // be handled properly and the system needs to be restarted
        return (jboolean) 0;
    }

    if (p_enableDebugThread) {
        g_debugThread = std::make_unique<ibnet::dx::DebugThread>(
            g_recvThread, g_sendThread);
        g_debugThread->Start();
    }

    IBNET_LOG_INFO("Initializing ibdxnet subsystem done");

	return (jboolean) 1;
}

JNIEXPORT jboolean JNICALL Java_de_hhu_bsinfo_dxnet_ib_JNIIbdxnet_shutdown(
        JNIEnv* p_env, jclass p_class)
{
    IBNET_LOG_TRACE_FUNC;

    jboolean res = (jboolean) 1;

    // TODO cleanup outgoing msg queues, make sure everything's processed?
    // TODO don't allow any new messages to be put to the send queue
    // wait until everything on the send queues is sent?

    if (g_debugThread) {
        g_debugThread->Stop();
        g_debugThread.reset();
    }

    g_sendThread->Stop();
    g_recvThread->Stop();

    try {
        g_connectionManager.reset();
        g_sharedRecvCompQueue.reset();
        g_sharedRecvQueue.reset();
        g_protDom.reset();
        g_device.reset();
    } catch (...) {
        res = (jboolean) 0;
    }

    g_connectionHandler.reset();
    g_recvHandler.reset();
    g_sendHandler.reset();

    g_sendThread.reset();
    g_recvThread.reset();

    ibnet::sys::Logger::Shutdown();

    g_signalHandler.reset();

    return res;
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_dxnet_ib_JNIIbdxnet_addNode(
        JNIEnv* p_env, jclass p_class, jint p_ipv4)
{
    IBNET_LOG_TRACE_FUNC;

    ibnet::core::IbNodeConf::Entry entry(
        ibnet::sys::AddressIPV4((uint32_t) p_ipv4));
    g_connectionManager->AddNode(entry);
}

JNIEXPORT jlong JNICALL Java_de_hhu_bsinfo_dxnet_ib_JNIIbdxnet_getSendBufferAddress(
        JNIEnv* p_env, jclass p_class, jshort p_targetNodeId)
{
    std::shared_ptr < ibnet::core::IbConnection > connection;

    try {
        connection = g_connectionManager->GetConnection(
                (uint16_t) (p_targetNodeId & 0xFFFF));
    } catch (ibnet::core::IbTimeoutException& e) {
        IBNET_LOG_ERROR("{}", e.what());
        return (jlong) -1;
    }

    if (!connection) {
        return (jlong) -1;
    }

    ibnet::core::IbMemReg* buffer = g_sendBuffers->GetBuffer(
        connection->GetConnectionId());

    g_connectionManager->ReturnConnection(connection);

    return (jlong) buffer->GetAddress();
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_dxnet_ib_JNIIbdxnet_returnRecvBuffer(
        JNIEnv* p_env, jclass p_class, jlong p_bufferHandle)
{
    ibnet::core::IbMemReg* mem = (ibnet::core::IbMemReg*) p_bufferHandle;

    IBNET_LOG_TRACE("Return recv buffer handle {:x}, buffer addr {:x}, size %d",
        p_bufferHandle, mem->GetAddress(), mem->GetSize());

    g_recvBufferPool->ReturnBuffer(mem);
}