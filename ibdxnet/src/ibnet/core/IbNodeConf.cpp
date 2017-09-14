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

#include "IbNodeConf.h"

#include "ibnet/sys/Network.h"

namespace ibnet {
namespace core {

IbNodeConf::Entry::Entry(void) :
    m_hostname("***INVALID***"),
    m_address()
{

}

IbNodeConf::Entry::Entry(const sys::AddressIPV4& ipv4) :
    m_hostname(),
    m_address(ipv4)
{

}

IbNodeConf::Entry::Entry(const std::string& hostname) :
    m_hostname(hostname),
    m_address(sys::Network::ResolveHostname(m_hostname))
{

}

IbNodeConf::Entry::~Entry(void)
{

}

IbNodeConf::IbNodeConf(void)
{

}

IbNodeConf::~IbNodeConf(void)
{

}

void IbNodeConf::AddEntry(const std::string& hostname)
{
    m_entries.push_back(Entry(hostname));
}

}
}