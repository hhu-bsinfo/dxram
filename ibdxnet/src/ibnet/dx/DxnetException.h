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

#ifndef IBNET_DX_DXNETEXCEPTION_H
#define IBNET_DX_DXNETEXCEPTION_H

#include "ibnet/sys/Exception.h"

namespace ibnet {
namespace dx {

/**
 * Common exception for the dxnet namespace
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.06.2017
 */
class DxnetException : public sys::Exception
{
public:
    /**
     * Constructor
     *
     * @param msg Exception message
     */
    DxnetException(const std::string& msg) :
            Exception(msg)
    {

    }

    /**
     * Destructor
     */
    ~DxnetException(void) {};
};

}
}

#endif // IBNET_DX_DXNETEXCEPTION_H
