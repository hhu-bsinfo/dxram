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

import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Exporter pool stores one exporter collection per thread.
 * Lock-free implementation with threadID as index in array.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 07.07.2017
 */
public final class StaticExporterPool extends AbstractExporterPool {

    private static final Logger LOGGER = LogManager.getFormatterLogger(StaticExporterPool.class.getSimpleName());
    private static final int SLOT_SIZE = 100;

    // Attributes
    private MessageExporterCollection[] m_exporters = new MessageExporterCollection[SLOT_SIZE];
    private ReentrantLock m_lock = new ReentrantLock(false);

    /**
     * Creates an instance of StaticExporterPool
     */
    public StaticExporterPool() {
    }

    @Override
    public MessageExporterCollection getInstance() {
        MessageExporterCollection ret;
        long threadID = Thread.currentThread().getId();

        if (threadID >= m_exporters.length) {
            m_lock.lock();
            if (threadID >= m_exporters.length) {
                if (m_exporters.length >= 10 * SLOT_SIZE) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Many threads actively sending messages (>%d). You might consider switching to dynamic exporter pool (configuration).",
                            m_exporters.length);
                    // #endif /* LOGGER >= WARN */
                }
                // Copying without lock might result in lost allocations but this can be ignored
                MessageExporterCollection[] tmp = new MessageExporterCollection[m_exporters.length + SLOT_SIZE];
                System.arraycopy(m_exporters, 0, tmp, 0, m_exporters.length);
                m_exporters = tmp;
            }
            m_lock.unlock();
        }

        ret = m_exporters[(int) threadID];
        if (ret == null) {
            ret = new MessageExporterCollection();

            m_exporters[(int) threadID] = ret;
        }

        return ret;
    }
}
