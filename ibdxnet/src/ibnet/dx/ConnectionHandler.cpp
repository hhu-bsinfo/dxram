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

#include "ConnectionHandler.h"

namespace ibnet {
namespace dx {

ConnectionHandler::ConnectionHandler(JNIEnv* env, jobject object,
        std::shared_ptr<RecvThread>& recvThread) :
    m_vm(nullptr),
    m_object(env->NewGlobalRef(object)),
    m_recvThread(recvThread),
    m_midNodeDiscovered(JNIHelper::GetAndVerifyMethod(env, object,
        "nodeDiscovered", "(S)V")),
    m_midNodeInvalidated(JNIHelper::GetAndVerifyMethod(env, object,
        "nodeInvalidated", "(S)V")),
    m_midNodeConnected(env->GetMethodID(env->GetObjectClass(object),
        "nodeConnected", "(S)V")),
    m_midNodeDisconnected(
        env->GetMethodID(env->GetObjectClass(object), "nodeDisconnected",
            "(S)V"))
{
    env->GetJavaVM(&m_vm);
}

ConnectionHandler::~ConnectionHandler(void)
{

}

}
}