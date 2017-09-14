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

#ifndef IBNET_DX_JNIHELPER_H
#define IBNET_DX_JNIHELPER_H

#include <jni.h>
#include <string.h>

#include <cstdint>
#include <stdexcept>

#include "ibnet/sys/Logger.hpp"

namespace ibnet {
namespace dx {

/**
 * Helper class for JNI related work
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 07.07.2017
 */
class JNIHelper
{
public:
    /**
     * Get the environment for the current thread from the jvm
     *
     * @param vm The jvm instance
     * @return Active environment for the current thread
     */
    static inline JNIEnv* GetEnv(JavaVM* vm)
    {
        //IBNET_LOG_TRACE_FUNC;

        JNIEnv* env;

        // Very important note:
        // If the JVM is crashing here: Have a look at the JNINotes.md file

        int envStat = vm->GetEnv((void **)&env, JNI_VERSION_1_8);
        if (envStat == JNI_EDETACHED) {
            if (vm->AttachCurrentThread((void **) &env, NULL) != 0) {
                throw std::runtime_error("Failed to attach to java vm");
            }
        } else if (envStat == JNI_OK) {
            // already attached to environment
        } else if (envStat == JNI_EVERSION) {
            throw std::runtime_error(
                "Failed to attach to java vm, jni version not supported");
        }

        return env;
    }

    /**
     * Return the environment received from GetEnv when done using it
     *
     * @param vm The jvm instance
     * @param env The environment to return
     */
    static inline void ReturnEnv(JavaVM* vm, JNIEnv* env)
    {
        //IBNET_LOG_TRACE_FUNC;

        // Don't check for exceptions because this is extremely expensive
        // and kills performance on recv callbacks
        // if (env->ExceptionCheck()) {
        //    env->ExceptionDescribe();
        // }

        // Don't detach. This is very expensive and increases the costs
        // for re-attaching a lot. The number of threads calling back to
        // the java context is limited, so we keep them attached
        // vm->DetachCurrentThread();
    }

    /**
     * Get the method id of a java method to call from a jvm environment
     *
     * @param env Environment of the jvm
     * @param object Instance of the class to get a method of
     * @param name Name of the method
     * @param signature Signature of the method
     * @return The method id of the specified java method
     */
    static inline jmethodID GetAndVerifyMethod(JNIEnv* env, jobject object,
            const std::string& name, const std::string& signature)
    {
        jmethodID mid;
        
        mid = env->GetMethodID(env->GetObjectClass(object), name.c_str(),
            signature.c_str());

        if (mid == 0) {
            IBNET_LOG_ERROR("Could not find method id of {}, {}",
                name, signature);
            throw std::runtime_error("Could not find method id of " + name +
                ", " + signature);
        }

        return mid;
    }

private:
    JNIHelper(void) {};
    ~JNIHelper(void) {};
};

}
}

#endif //IBNET_DX_JNIHELPER_H
