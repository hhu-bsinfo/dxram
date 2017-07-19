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

import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.UnfinishedImporterOperation;

/**
 * Implementation of an Importer/Exporter for ByteBuffers.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
// TODO can be merged with NIO variant into base class
class IBMessageImporterCollection {

    private IBMessageImporter m_importer;
    private IBMessageImporterOverflow m_importerOverflow;
    private IBMessageImporterUnderflow m_importerUnderflow;

    private UnfinishedImporterOperation m_unfinishedOperation;

    private int m_bytesCopied;

    /**
     * Constructor
     */
    IBMessageImporterCollection() {
        super();

        m_unfinishedOperation = new UnfinishedImporterOperation();

        m_importer = new IBMessageImporter();
        m_importerOverflow = new IBMessageImporterOverflow(m_unfinishedOperation);
        m_importerUnderflow = new IBMessageImporterUnderflow(m_unfinishedOperation);

        m_bytesCopied = 0;
    }

    AbstractMessageImporter getImporter(final long p_addr, final int p_size, final int p_position, final boolean p_hasOverflow) {
        AbstractMessageImporter ret;

        if (m_bytesCopied != 0) {
            // System.out.println("Using importer underflow");
            m_importerUnderflow.setBuffer(p_addr, p_size, p_position);
            ret = m_importerUnderflow;
        } else if (p_hasOverflow) {
            // System.out.println("Using importer overflow");
            m_importerOverflow.setBuffer(p_addr, p_size, p_position);
            ret = m_importerOverflow;
        } else {
            m_importer.setBuffer(p_addr, p_size, p_position);
            ret = m_importer;
        }
        ret.setNumberOfReadBytes(m_bytesCopied);

        return ret;
    }

    void returnImporter(final AbstractMessageImporter p_importer, final boolean p_finished) {
        if (p_finished) {
            m_bytesCopied = 0;
            m_unfinishedOperation.reset();
        } else {
            m_bytesCopied = p_importer.getNumberOfReadBytes();
        }
    }

}
