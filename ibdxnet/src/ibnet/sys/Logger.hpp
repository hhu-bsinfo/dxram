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

#ifndef IBNET_SYS_LOGGER_HPP
#define IBNET_SYS_LOGGER_HPP

#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include "ibnet/sys/Logger.h"

/**
 * Put this at the top of each file you call logging functions in and define
 * a name for the module. This name is added to the log strings on log calls
 */
#ifndef LOG_MODULE
#define S1(x) #x
#define S2(x) S1(x)
#define LOG_MODULE strrchr(__FILE__ ":" S2(__LINE__), '/') ? \
    strrchr(__FILE__ ":" S2(__LINE__), '/') + 1 : __FILE__ ":" S2(__LINE__)
#endif

/**
 * Macro to log a message, level panic. Use this macro instead of directly
 * calling the logger class.
 */
#define IBNET_LOG_PANIC(fmt, ...) ibnet::sys::Logger::GetLogger()->critical(("[{}] " + std::string(fmt)).c_str(), LOG_MODULE, ##__VA_ARGS__)

/**
 * Macro to log a message, level error. Use this macro instead of directly
 * calling the logger class.
 */
#define IBNET_LOG_ERROR(fmt, ...) ibnet::sys::Logger::GetLogger()->error(("[{}] " + std::string(fmt)).c_str(), LOG_MODULE, ##__VA_ARGS__)

/**
 * Macro to log a message, level warning. Use this macro instead of directly
 * calling the logger class.
 */
#define IBNET_LOG_WARN(fmt, ...) ibnet::sys::Logger::GetLogger()->warn(("[{}] " + std::string(fmt)).c_str(), LOG_MODULE, ##__VA_ARGS__)

/**
 * Macro to log a message, level info. Use this macro instead of directly
 * calling the logger class.
 */
#define IBNET_LOG_INFO(fmt, ...) ibnet::sys::Logger::GetLogger()->info(("[{}] " + std::string(fmt)).c_str(), LOG_MODULE, ##__VA_ARGS__)

/**
 * Macro to log a message, level debug. Use this macro instead of directly
 * calling the logger class.
 */
#define IBNET_LOG_DEBUG(fmt, ...) ibnet::sys::Logger::GetLogger()->debug(("[{}] " + std::string(fmt)).c_str(), LOG_MODULE, ##__VA_ARGS__)


#define IBNET_LOG_TRACE_STRIP
#ifndef IBNET_LOG_TRACE_STRIP

/**
 * Macro to log a message, level trace. Use this macro instead of directly
 * calling the logger class.
 */
#define IBNET_LOG_TRACE(fmt, ...) ibnet::sys::Logger::GetLogger()->trace(("[{}] " + std::string(fmt)).c_str(), LOG_MODULE, ##__VA_ARGS__)

/**
 * Macro to easily trace function calls. Just add this at the top of a
 * function's body.
 */
#define IBNET_LOG_TRACE_FUNC IBNET_LOG_TRACE("{} {}", "ENTER", __PRETTY_FUNCTION__)

/**
 * Macro to easily trace function calls. Just add this at the bottom of a
 * function's body.
 */
#define IBNET_LOG_TRACE_FUNC_EXIT IBNET_LOG_TRACE("{} {}", "EXIT", __PRETTY_FUNCTION__)
#else
#define IBNET_LOG_TRACE(fmt, ...)
#define IBNET_LOG_TRACE_FUNC
#define IBNET_LOG_TRACE_FUNC_EXIT
#endif

#endif