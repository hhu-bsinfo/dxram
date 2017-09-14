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

#ifndef IBNET_SYS_THREAD_H
#define IBNET_SYS_THREAD_H

#include <chrono>
#include <thread>

#include "Exception.h"
#include "Logger.hpp"

namespace ibnet {
namespace sys {

/**
 * Thread base class based on std::thread with additional features
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class Thread
{
public:
    /**
     * Constructor
     *
     * @param name Name of the thread (for debugging)
     */
    Thread(const std::string& name = "") :
        m_name(name),
        m_thread(nullptr)
    {}

    /**
     * Destructor
     */
    virtual ~Thread(void)
    {

    }

    /**
     * Get the thread name
     */
    const std::string& GetName(void) const {
        return m_name;
    }

    /**
     * Start the thread. A thread can be restarted if it finished execution
     */
    void Start(void)
    {
        m_thread = std::make_unique<std::thread>(&Thread::__Run, this);
    }

    /**
     * Join the started thread
     */
    void Join(void)
    {
        m_thread->join();
        m_thread.reset();
    }

protected:
    /**
     * Method executed by new thread. Implement this
     */
    virtual void _Run(void) = 0;

    /**
     * Yield this thread
     */
    void _Yield(void)
    {
        std::this_thread::yield();
    }

    /**
     * Put thread to sleep
     * @param timeMs Number of ms to sleep
     */
    void _Sleep(uint32_t timeMs)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(timeMs));
    }

private:
    const std::string m_name;
    std::unique_ptr<std::thread> m_thread;

    void __Run(void)
    {
        try {
            IBNET_LOG_INFO("Started thread {}", m_name);
            _Run();
            IBNET_LOG_INFO("Finished thread {}", m_name);
        } catch (Exception& e) {
            e.PrintStackTrace();
            throw e;
        }
    }
};

}
}

#endif //IBNET_SYS_THREAD_H
