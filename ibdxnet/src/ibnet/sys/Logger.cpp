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

#include "Logger.h"

namespace ibnet {
namespace sys {

std::shared_ptr<spdlog::logger> Logger::m_logger;

void Logger::Setup(void)
{
    auto colorSink = std::make_shared<spdlog::sinks::ansicolor_sink>(
        std::make_shared<spdlog::sinks::stdout_sink_mt>());
    colorSink->set_color(spdlog::level::trace, colorSink->white);
    colorSink->set_color(spdlog::level::debug, colorSink->green);
    colorSink->set_color(spdlog::level::info,
        colorSink->bold + colorSink->blue);
    colorSink->set_color(spdlog::level::warn,
        colorSink->bold + colorSink->yellow);
    colorSink->set_color(spdlog::level::err, colorSink->bold + colorSink->red);
    colorSink->set_color(spdlog::level::critical,
        colorSink->bold + colorSink->on_red);
    colorSink->set_color(spdlog::level::off, colorSink->reset);

//    sinks.push_back(std::make_shared<spdlog::sinks::simple_file_sink_mt>(
//        "ibnet.log", true));

    m_logger =
        std::make_shared<spdlog::logger>("ibnet", colorSink);
    m_logger->set_pattern("[%L][%D][%T.%e][thread-%t]%v");

    m_logger->set_level(spdlog::level::trace);

    m_logger->flush_on(spdlog::level::info);
}

void Logger::Shutdown(void)
{
    m_logger.reset();
}

}
}