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

#ifndef IBNET_DX_CONNECTIONHANDLER_H
#define IBNET_DX_CONNECTIONHANDLER_H

#include "ibnet/core/IbConnectionManager.h"

#include "JNIHelper.h"
#include "RecvThread.h"

namespace ibnet {
namespace dx {

/**
 * Handle node connect/disconnects by calling back to the jvm space
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 07.07.2017
 */
class ConnectionHandler : public ibnet::core::IbConnectionManager::Listener
{
public:
    /**
     * Constructor
     *
     * @param env The java environment from a java thread
     * @param object Java object of the equivalent callback class in java
     * @param recvThread Pointer to the receive thread
     */
    ConnectionHandler(JNIEnv* env, jobject object,
        std::shared_ptr<RecvThread>& recvThread);
    ~ConnectionHandler(void);

    /**
     * Override
     */
    void NodeConnected(uint16_t nodeId,
                       ibnet::core::IbConnection &connection) override
    {
        m_recvThread->NodeConnected(connection);

        __NodeConnected(nodeId);
    }

    /**
     * Override
     */
    void NodeDisconnected(uint16_t nodeId) override
    {
        __NodeDisconnected(nodeId);
    }

    /**
     * Override
     */
    void NodeDiscovered(uint16_t nodeId) override
    {
        __NodeDiscovered(nodeId);
    }

    /**
     * Override
     */
    void NodeInvalidated(uint16_t nodeId) override
    {
        __NodeInvalidated(nodeId);
    }

private:
    inline void __NodeDiscovered(uint16_t nodeId)
    {
        IBNET_LOG_TRACE_FUNC;

        JNIEnv* env = JNIHelper::GetEnv(m_vm);
        env->CallVoidMethod(m_object, m_midNodeDiscovered, nodeId);
        JNIHelper::ReturnEnv(m_vm, env);

        IBNET_LOG_TRACE_FUNC_EXIT;
    }

    inline void __NodeInvalidated(uint16_t nodeId)
    {
        IBNET_LOG_TRACE_FUNC;

        JNIEnv* env = JNIHelper::GetEnv(m_vm);
        env->CallVoidMethod(m_object, m_midNodeInvalidated, nodeId);
        JNIHelper::ReturnEnv(m_vm, env);

        IBNET_LOG_TRACE_FUNC_EXIT;
    }

    inline void __NodeConnected(uint16_t nodeId)
    {
        IBNET_LOG_TRACE_FUNC;

        JNIEnv* env = JNIHelper::GetEnv(m_vm);
        env->CallVoidMethod(m_object, m_midNodeConnected, nodeId);
        JNIHelper::ReturnEnv(m_vm, env);

        IBNET_LOG_TRACE_FUNC_EXIT;
    }

    inline void __NodeDisconnected(uint16_t nodeId)
    {
        IBNET_LOG_TRACE_FUNC;

        JNIEnv* env = JNIHelper::GetEnv(m_vm);
        env->CallVoidMethod(m_object, m_midNodeDisconnected, nodeId);
        JNIHelper::ReturnEnv(m_vm, env);

        IBNET_LOG_TRACE_FUNC_EXIT;
    }

private:
    JavaVM* m_vm;
    jobject m_object;

    std::shared_ptr<RecvThread>& m_recvThread;

    jmethodID m_midNodeDiscovered;
    jmethodID m_midNodeInvalidated;
    jmethodID m_midNodeConnected;
    jmethodID m_midNodeDisconnected;
};

}
}

#endif //IBNET_DX_CONNECTIONHANDLER_H
