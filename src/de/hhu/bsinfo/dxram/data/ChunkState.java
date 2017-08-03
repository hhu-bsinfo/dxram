/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.data;

/**
 * Different states for a chunk or data structure. DXRAM operations set the state of a chunk/data structure to indicate failure/success
 * and allow the user to handle errors
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 28.03.2017
 */
public enum ChunkState {
    /**
     * When chunk object is created but wasn't used with any call to a chunk service, yet
     */
    UNDEFINED,

    /**
     * The recently executed operation by DXRAM involving the chunk/data structure was successful
     */
    OK,

    /**
     * The chunk ID was invalid (-1) on the recently executed operation
     */
    INVALID_ID,

    /**
     * Data for the chunk/data structure did not exist on the last operation
     */
    DOES_NOT_EXIST,

    /**
     * Data for the chunk/data structure was temporary unavailable on the last operation (migration or recovery in progress)
     */
    DATA_TEMPORARY_UNAVAILABLE,

    /**
     * Data for the chunk/data structure is lost due to node failure and disabled backup/recovery
     */
    DATA_LOST,
}
