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

package de.hhu.bsinfo.dxnet.core;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Exporter pool stores one exporter collection per thread.
 * Is based on a hash map. Clears all entries if hash map becomes too large.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 07.07.2017
 */
public final class DynamicExporterPool extends AbstractExporterPool {

    private static final Logger LOGGER = LogManager.getFormatterLogger(DynamicExporterPool.class.getSimpleName());
    private static final int THRESHOLD = 1000;

    // Attributes
    private HashMap<Long, MessageExporterCollection> m_exporters = new HashMap<>();
    private ReentrantLock m_lock = new ReentrantLock(false);

    /**
     * Creates an instance of DynamicExporterPool
     */
    public DynamicExporterPool() {
    }

    @Override
    public MessageExporterCollection getInstance() {
        MessageExporterCollection ret;
        long threadID = Thread.currentThread().getId();

        m_lock.lock();
        ret = m_exporters.get(threadID);
        if (ret == null) {
            if (m_exporters.size() > THRESHOLD) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Many threads actively sending messages (>%d). Clearing hash map to reduce memory overhead.", m_exporters.size());
                // #endif /* LOGGER >= DEBUG */
            }

            ret = new MessageExporterCollection();
            m_exporters.put(threadID, ret);
        }
        m_lock.unlock();

        return ret;
    }
}
