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

#include "RecvHandler.h"

namespace ibnet {
namespace dx {

RecvHandler::RecvHandler(JNIEnv* env, jobject object) :
    m_vm(nullptr),
    m_object(env->NewGlobalRef(object)),
    m_midReceivedBuffer(JNIHelper::GetAndVerifyMethod(env, m_object,
        "receivedBuffer", "(SJJI)V")),
    m_midReceivedFlowControlData(JNIHelper::GetAndVerifyMethod(env, m_object,
        "receivedFlowControlData", "(SI)V")),
    m_directBufferAddressField(env->GetFieldID(
        env->FindClass("java/nio/Buffer"), "address", "J"))
{
    env->GetJavaVM(&m_vm);
}

RecvHandler::~RecvHandler(void)
{

}

}
}