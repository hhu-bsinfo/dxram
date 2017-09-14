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

#include "IbNodeConfArgListReader.h"

namespace ibnet {
namespace core {

IbNodeConfArgListReader::IbNodeConfArgListReader(uint32_t numItems,
        char** args) :
    IbNodeConfReader(),
    m_numItems(numItems),
    m_args(args)
{

}

IbNodeConfArgListReader::~IbNodeConfArgListReader(void)
{

}

IbNodeConf IbNodeConfArgListReader::Read(void)
{
    IbNodeConf conf;

    for (uint32_t i = 0; i < m_numItems; i++) {
        conf.AddEntry(m_args[i]);
    }

    return conf;
}

}
}