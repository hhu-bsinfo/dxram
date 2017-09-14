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

#ifndef IBNET_DX_SENDHANDLER_H
#define IBNET_DX_SENDHANDLER_H

#include "ibnet/core/IbNodeId.h"

#include "JNIHelper.h"

namespace ibnet {
namespace dx {

/**
 * Provide access to buffers which are available in the jvm space. This
 * is called by the SendThread to get new/next buffers to send from java
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 07.07.2017
 */
class SendHandler
{
public:
    /**
     * A work request package that defines which data to be sent next.
     * Returned using a pointer by the callback from the java space
     */
    struct NextWorkParameters
    {
        uint32_t m_posFrontRel;
        uint32_t m_posBackRel;
        uint32_t m_flowControlData;
        uint16_t m_nodeId;
    } __attribute__((packed));

public:
    /**
     * Constructor
     *
     * @param env The java environment from a java thread
     * @param object Java object of the equivalent callback class in java
     */
    SendHandler(JNIEnv* env, jobject object);

    /**
     * Destructor
     */
    ~SendHandler(void);

    /**
     * Called by an instance of SendThread. Get the next buffer/work package
     * to send
     *
     * @param prevNodeIdWritten Node id of the previous next call
     *        (or -1 if there is no previous valid work request)
     * @param prevDataWrittenLen Number of bytes written on the previous work
     *          request
     * @return Pointer to the next work request package (don't free)
     */
    inline NextWorkParameters* GetNextDataToSend(uint16_t prevNodeIdWritten,
            uint32_t prevDataWrittenLen)
    {
        // IBNET_LOG_TRACE_FUNC;

        JNIEnv* env = JNIHelper::GetEnv(m_vm);
        jlong ret = env->CallLongMethod(m_object, m_midGetNextDataToSend,
            // odd: 0xFFFF is not reinterpreted as -1 when assigned to a jshort
            prevNodeIdWritten == core::IbNodeId::INVALID ? -1 : prevNodeIdWritten,
            prevDataWrittenLen);
        JNIHelper::ReturnEnv(m_vm, env);

        // IBNET_LOG_TRACE_FUNC_EXIT;

        return (NextWorkParameters*) ret;
    }

private:
    JavaVM* m_vm;
    jobject m_object;

    jmethodID m_midGetNextDataToSend;
};

}
}

#endif //IBNET_DX_SENDHANDLER_H
