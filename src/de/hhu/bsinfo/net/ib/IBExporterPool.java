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

package de.hhu.bsinfo.net.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.NetworkRuntimeException;

/**
 * @author Kevin Beineke, kevin.beineke@hhu.de, 07.07.2017
 */
public final class IBExporterPool {

    private static final Logger LOGGER = LogManager.getFormatterLogger(IBExporterPool.class.getSimpleName());
    private static final int SLOT_SIZE = 100;

    // Attributes
    private static IBMessageExporterCollection[] ms_exporters = new IBMessageExporterCollection[SLOT_SIZE];

    private IBExporterPool() {
    }

    public static IBMessageExporterCollection getInstance() {
        IBMessageExporterCollection ret;
        long threadID = Thread.currentThread().getId();

        if (threadID >= ms_exporters.length) {
            if (ms_exporters.length == 10 * SLOT_SIZE) {
                // TODO

                throw new NetworkRuntimeException("ThreadID is too high. Change configuration...");
            } else {
                // Copying without lock might result in lost allocations but this can be ignored
                IBMessageExporterCollection[] tmp = new IBMessageExporterCollection[ms_exporters.length + SLOT_SIZE];
                System.arraycopy(ms_exporters, 0, tmp, 0, ms_exporters.length);
                ms_exporters = tmp;
            }
        }

        ret = ms_exporters[(int) threadID];
        if (ret == null) {
            ret = new IBMessageExporterCollection();

            ms_exporters[(int) threadID] = ret;
        }

        return ret;
    }
}
