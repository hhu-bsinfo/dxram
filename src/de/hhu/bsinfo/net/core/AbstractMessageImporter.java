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

package de.hhu.bsinfo.net.core;

import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Abstraction of an Importer for network messages.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.07.2017
 */
public abstract class AbstractMessageImporter implements Importer {

    /**
     * Constructor
     */
    protected AbstractMessageImporter() {
    }

    /**
     * Get current position in byte array
     *
     * @return the position
     */
    protected abstract int getPosition();

    /**
     * Get the number of de-serialized bytes.
     *
     * @return number of read bytes
     */
    public abstract int getNumberOfReadBytes();

    /**
     * Set the number of read bytes. Only relevant for underflow importer to skip already finished operations.
     *
     * @param p_numberOfReadBytes
     *         the number of read bytes
     */
    public abstract void setNumberOfReadBytes(int p_numberOfReadBytes);
}
