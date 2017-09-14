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

#ifndef IBNET_CORE_IBNODECONFARGLISTREADER_H
#define IBNET_CORE_IBNODECONFARGLISTREADER_H

#include "IbNodeConfReader.h"

namespace ibnet {
namespace core {

/**
 * Implementation of a IbNodeConfReader to read a node config from an
 * argument list (main entry point).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbNodeConfArgListReader : public IbNodeConfReader
{
public:
    /**
     * Constructor
     *
     * @param numItems Number of elements from cmd arguments
     * @param args Arguments
     */
    IbNodeConfArgListReader(uint32_t numItems, char** args);

    /**
     * Destructor
     */
    ~IbNodeConfArgListReader(void);

    IbNodeConf Read(void) override;

private:
    uint32_t m_numItems;
    char** m_args;
};

}
}

#endif // IBNET_CORE_IBNODECONFARGLISTREADER_H
