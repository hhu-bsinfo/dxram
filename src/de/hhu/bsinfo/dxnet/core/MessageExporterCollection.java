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

/**
 * Exporter collection.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class MessageExporterCollection {

    private MessageExporterDefault m_exporter;
    private MessageExporterOverflow m_exporterOverflow;
    private LargeMessageExporter m_exporterLargeMessage;

    // for debugging
    private AbstractMessageExporter m_lastExporterUsed;

    private UnfinishedImExporterOperation m_unfinishedOperation;

    /**
     * Constructor
     */
    MessageExporterCollection() {
        super();

        m_exporter = new MessageExporterDefault();
        m_exporterOverflow = new MessageExporterOverflow();

        m_unfinishedOperation = new UnfinishedImExporterOperation();
        m_exporterLargeMessage = new LargeMessageExporter(m_unfinishedOperation);
    }

    @Override
    public String toString() {
        return "m_lastExporterUsed(" + m_lastExporterUsed.getClass().getSimpleName() + "): [" + m_lastExporterUsed + "], m_exporter [" + m_exporter +
                "], m_exporterOverflow [" + m_exporterOverflow + "], m_exporterLargeMessage [" + m_exporterLargeMessage + "], m_unfinishedOperation [" +
                m_unfinishedOperation + ']';
    }

    /**
     * Get corresponding exporter (default, overflow)
     *
     * @param p_hasOverflow
     *         whether all bytes to export fit in buffer
     * @return the AbstractMessageExporter
     */
    AbstractMessageExporter getMessageExporter(final boolean p_hasOverflow) {
        if (!p_hasOverflow) {
            m_lastExporterUsed = m_exporter;
            return m_exporter;
        } else {
            m_lastExporterUsed = m_exporterOverflow;
            return m_exporterOverflow;
        }
    }

    /**
     * Get corresponding exporter for large messages
     *
     * @return the LargeMessageExporter
     */
    LargeMessageExporter getLargeMessageExporter() {
        return m_exporterLargeMessage;
    }

    /**
     * Reset unfinished operation for next large message.
     */
    void deleteUnfinishedOperation() {
        m_unfinishedOperation.reset();
    }
}
