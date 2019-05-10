/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.loader.messages;

import lombok.Getter;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.loader.LoaderJar;

/**
 * @author Julien Bernhart, julien.bernhart@hhu.de, 2019-04-17
 */
public class DistributeJarMessage extends Message {
    @Getter
    private LoaderJar m_loaderJar;
    @Getter
    private int m_tableSize;

    public DistributeJarMessage() {
        super();
        m_loaderJar = new LoaderJar();
    }

    public DistributeJarMessage(final short p_destination, final LoaderJar p_loaderJar, final int p_tableSize) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_DISTRIBUTE);
        m_loaderJar = p_loaderJar;
        m_tableSize = p_tableSize;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += m_loaderJar.sizeofObject();
        size += Integer.BYTES;

        return size;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        m_loaderJar.exportObject(p_exporter);
        p_exporter.writeInt(m_tableSize);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_loaderJar.importObject(p_importer);
        m_tableSize = p_importer.readInt(m_tableSize);
    }
}
