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

import de.hhu.bsinfo.utils.serialization.Exporter;

/**
 * Implementation of an Importer/Exporter for ByteBuffers.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public abstract class AbstractMessageExporter implements Exporter {

    /**
     * Constructor
     */
    protected AbstractMessageExporter() {
    }

    protected abstract int getNumberOfWrittenBytes();

    protected abstract void setBuffer(byte[] p_buffer);

    protected abstract void setPosition(int p_position);

}
