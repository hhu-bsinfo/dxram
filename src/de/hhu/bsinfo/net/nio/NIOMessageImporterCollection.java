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

package de.hhu.bsinfo.net.nio;

import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporterCollection;

/**
 * Importer collection.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
public class NIOMessageImporterCollection extends AbstractMessageImporterCollection {

    private NIOMessageImporter m_importer;
    private NIOMessageImporterOverflow m_importerOverflow;
    private NIOMessageImporterUnderflow m_importerUnderflow;
    private NIOMessageImporterUnderOverflow m_importerUnderOverflow;

    private UnfinishedOperation m_unfinishedOperation;

    private int m_bytesCopied;

    /**
     * Constructor
     */
    public NIOMessageImporterCollection() {
        super();

        m_unfinishedOperation = new UnfinishedOperation();

        m_importer = new NIOMessageImporter();
        m_importerOverflow = new NIOMessageImporterOverflow(m_unfinishedOperation);
        m_importerUnderflow = new NIOMessageImporterUnderflow(m_unfinishedOperation);
        m_importerUnderOverflow = new NIOMessageImporterUnderOverflow(m_unfinishedOperation);

        m_bytesCopied = 0;
    }

    @Override
    public AbstractMessageImporter getImporter(final boolean p_hasOverflow) {
        AbstractMessageImporter ret;

        if (m_bytesCopied != 0) {
            /*if (p_hasOverflow) {
                ret = m_importerUnderOverflow;
            } else {*/
            ret = m_importerUnderflow;
            //}
        } else if (p_hasOverflow) {
            ret = m_importerOverflow;
        } else {
            ret = m_importer;
        }
        ret.setNumberOfReadBytes(m_bytesCopied);

        return ret;
    }

    @Override
    protected void returnImporter(final AbstractMessageImporter p_importer, final boolean p_finished) {
        if (p_finished) {
            m_bytesCopied = 0;
            m_unfinishedOperation.reset();
        } else {
            m_bytesCopied = p_importer.getNumberOfReadBytes();
        }
    }

}
