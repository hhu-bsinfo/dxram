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

#ifndef IBNET_SYS_LOGGER_H
#define IBNET_SYS_LOGGER_H

#include <spdlog/spdlog.h>

namespace ibnet {
namespace sys {

/**
 * Logger (wrapper) class to log errors, warnings, debug messages etc
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class Logger
{
public:
    /**
     * Setup the logger. Make sure to call this to enable the logger
     */
    static void Setup(void);

    /**
     * Shutdown and cleanup everything. Call this before your application
     * exits to properly close and flush everything
     */
    static void Shutdown(void);

    /**
     * Get the logger
     */
    static std::shared_ptr<spdlog::logger>& GetLogger() {
        return m_logger;
    }

private:
    Logger(void) {};
    ~Logger(void) {};

    static std::shared_ptr<spdlog::logger> m_logger;
};

}
}

#endif //IBNET_SYS_LOGGER_H
