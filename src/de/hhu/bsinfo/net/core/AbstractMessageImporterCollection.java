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

/**
 * Implementation of an Importer/Exporter for ByteBuffers.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public abstract class AbstractMessageImporterCollection {

    /**
     * Constructor
     */
    protected AbstractMessageImporterCollection() {
    }

    protected abstract AbstractMessageImporter getImporter(boolean p_hasOverflow);

    protected abstract void returnImporter(AbstractMessageImporter p_importer, boolean p_finished);

    public class UnfinishedOperation {
        private int m_startIndex;
        private long m_primitive;
        private Object m_object;

        public UnfinishedOperation() {

        }

        public int getIndex() {
            return m_startIndex;
        }

        public long getPrimitive() {
            return m_primitive;
        }

        public Object getObject() {
            return m_object;
        }

        public void setIndex(final int p_index) {
            m_startIndex = p_index;
        }

        public void setPrimitive(final long p_primitive) {
            m_primitive = p_primitive;
        }

        public void setObject(final Object p_object) {
            m_object = p_object;
        }

        public void reset() {
            m_primitive = 0;
            m_object = null;
            m_startIndex = 0;
        }
    }

}
