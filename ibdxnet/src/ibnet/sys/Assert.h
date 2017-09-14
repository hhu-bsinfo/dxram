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

#ifndef IBNET_SYS_ASSERT_H
#define IBNET_SYS_ASSERT_H

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#ifndef ASSERT_MODULE
#define ASSERT_MODULE __FILE__
#endif

/**
 * Assert macro
 */
#define IBNET_ASSERT(expr) \
    assert(expr)

/**
 * Assert and if fails exit program
 */
#define IBNET_ASSERT_DIE(msg) \
    printf("%s", msg); \
    abort()

/**
 * Assert nullptr check
 */
#define IBNET_ASSERT_PTR(ptr) \
    assert(ptr != NULL)

/**
 * Assert if pointer is null
 */
#define IBNET_ASSERT_PTR_NULL(ptr) \
    assert(ptr == NULL)

#endif // IBNET_SYS_ASSERT_H
