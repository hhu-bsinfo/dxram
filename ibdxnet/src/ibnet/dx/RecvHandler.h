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

#ifndef IBNET_DX_RECVHANDLER_H
#define IBNET_DX_RECVHANDLER_H

#include "ibnet/core/IbMemReg.h"

#include "JNIHelper.h"

namespace ibnet {
namespace dx {

/**
 * Handle received buffers and pass them into the jvm space
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 07.07.2017
 */
class RecvHandler
{
public:
    /**
     * Constructor
     *
     * @param env The java environment from a java thread
     * @param object Java object of the equivalent callback class in java
     */
    RecvHandler(JNIEnv* env, jobject object);

    /**
     * Destructor
     */
    ~RecvHandler(void);

    /**
     * Called when a new buffer was received
     *
     * @param source The source node id
     * @param mem Pointer to the IbMemReg object of the buffer
     * @param buffer Pointer to the buffer with the received data
     * @param length Number of bytes received
     */
    inline void ReceivedBuffer(uint16_t source, core::IbMemReg* mem,
            void* buffer, uint32_t length)
    {
        IBNET_LOG_TRACE_FUNC;

        JNIEnv* env = JNIHelper::GetEnv(m_vm);

        env->CallVoidMethod(m_object, m_midReceivedBuffer, source,
            (uintptr_t) mem, buffer, length);

        JNIHelper::ReturnEnv(m_vm, env);

        IBNET_LOG_TRACE_FUNC_EXIT;
    }

    /**
     * Called when new flow control data was received
     *
     * @param source The source node id
     * @param data Flow control data received
     */
    inline void ReceivedFlowControlData(uint16_t source, uint32_t data)
    {
        IBNET_LOG_TRACE_FUNC;

        JNIEnv* env = JNIHelper::GetEnv(m_vm);
        env->CallVoidMethod(m_object, m_midReceivedFlowControlData, source,
            data);
        JNIHelper::ReturnEnv(m_vm, env);

        IBNET_LOG_TRACE_FUNC_EXIT;
    }

private:
    JavaVM* m_vm;
    jobject m_object;

    jmethodID m_midReceivedBuffer;
    jmethodID m_midReceivedFlowControlData;

    jfieldID m_directBufferAddressField;
};

}
}

#endif //IBNET_DX_RECVHANDLER_H
