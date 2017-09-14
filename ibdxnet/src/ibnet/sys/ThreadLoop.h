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

#ifndef IBNET_SYS_THREADLOOP_H
#define IBNET_SYS_THREADLOOP_H

#include <atomic>

#include "Thread.h"

namespace ibnet {
namespace sys {

/**
 * Typical usage of a thread looping on a field until the field turns false
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class ThreadLoop : public Thread
{
public:
    /**
     * Constructor
     *
     * @param name Name of the thread (for debugging)
     */
    ThreadLoop(const std::string& name = "") :
        Thread(name),
        m_run(true)
    {}

    /**
     * Destructor
     */
    virtual ~ThreadLoop(void)
    {}

    /**
     * Signal the thread to stop. This will cause the thread to exit
     * the loop before continuing with the next iteration.
     */
    void Stop(void)
    {
        m_run = false;
        Join();
    }

protected:
    /**
     * Execute something before the thread starts looping (e.g. init/setup)
     */
    virtual void _BeforeRunLoop(void) {};

    /**
     * Run function which is looped until exit is signaled
     */
    virtual void _RunLoop(void) = 0;

    /**
     * Execute something after the loop exited and before the thread terminates
     * (e.g. cleanup)
     */
    virtual void _AfterRunLoop(void) {};

    void exitLoop(void) {
        m_run = false;
    }

    void _Run(void) override
    {
        _BeforeRunLoop();

        while (m_run) {
            _RunLoop();
        }

        _AfterRunLoop();
    }

private:
    std::atomic<bool> m_run;
};

}
}

#endif //IBNET_SYS_THREADLOOP_H
