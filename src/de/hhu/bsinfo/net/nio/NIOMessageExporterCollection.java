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

import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageExporterCollection;

/**
 * Exporter collection.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
public class NIOMessageExporterCollection extends AbstractMessageExporterCollection {

    private AbstractMessageExporter m_exporter;
    private AbstractMessageExporter m_exporterWithOverflow;

    /**
     * Constructor
     */
    public NIOMessageExporterCollection() {
        super();
        m_exporter = new NIOMessageExporter();
        m_exporterWithOverflow = new NIOMessageExporterOverflow();
    }

    @Override
    public AbstractMessageExporter getExporter(final boolean p_hasOverflow) {
        if (!p_hasOverflow) {
            return m_exporter;
        } else {
            return m_exporterWithOverflow;
        }
    }

}
