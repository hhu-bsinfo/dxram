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

#ifndef IBNET_DX_DEBUGTHREAD_H
#define IBNET_DX_DEBUGTHREAD_H

#include "ibnet/sys/ThreadLoop.h"

#include "RecvThread.h"
#include "SendThread.h"

namespace ibnet {
namespace dx {

/**
 * Print performance and debug data periodically
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 07.07.2017
 */
class DebugThread : public sys::ThreadLoop
{
public:
    /**
     * Constructor
     *
     * @param recvThread Pointer to receive thread
     * @param sendThread Pointer to send thread
     */
    DebugThread(
        std::shared_ptr<RecvThread> recvThread,
        std::shared_ptr<SendThread> sendThread);

    /**
     * Destructor
     */
    ~DebugThread(void);

protected:
    void _RunLoop(void) override;

private:
    const std::shared_ptr<RecvThread> m_recvThread;
    const std::shared_ptr<SendThread> m_sendThread;
};

}
}

#endif //IBNET_DX_DEBUGTHREAD_H
