/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.lookup;

/**
 * Different states for a chunk ID lookup. The state is set during lookup over superpeer overlay to indicate failure/success
 * and allow to handle errors
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 29.05.2017
 */
public enum LookupState {
    /**
     * Lookup was successful
     */
    OK,

    /**
     * ChunkID does not exist
     */
    DOES_NOT_EXIST,

    /**
     * Recovery is in progress
     */
    DATA_TEMPORARY_UNAVAILABLE,

    /**
     * Data for the chunk/data structure is lost due to node failure and disabled backup/recovery
     */
    DATA_LOST,
}
