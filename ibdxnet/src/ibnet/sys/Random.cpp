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

#include "Random.h"

#include <stdlib.h>
#include <time.h>

namespace ibnet {
namespace sys {

bool Random::ms_randInit = false;

uint16_t Random::Generate16(void)
{
    return (uint16_t) Generate32();
}

uint32_t Random::Generate32(void)
{
    if (!ms_randInit) {
        srand48(time(NULL));
        ms_randInit = true;
    }

    return lrand48();
}

}
}