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

#ifndef IBNET_CORE_IBNODECONFSTRINGREADER_H
#define IBNET_CORE_IBNODECONFSTRINGREADER_H

#include "IbNodeConfReader.h"

namespace ibnet {
namespace core {

/**
 * Implementation of a IbNodeConfReader reading a nodes configuration from
 * a string
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbNodeConfStringReader : public IbNodeConfReader
{
public:
    /**
     * Constructor
     *
     * @param str String containing the nodes configuration,
     *          e.g. "node65 node66"
     */
    IbNodeConfStringReader(const std::string& str);

    /**
     * Destructor
     */
    ~IbNodeConfStringReader(void);

    IbNodeConf Read(void) override;

private:
    const std::string& m_str;
};

}
}


#endif // IBNET_CORE_IBNODECONFSTRINGREADER_H
