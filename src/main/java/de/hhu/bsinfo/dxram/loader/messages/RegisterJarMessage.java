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
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class RegisterJarMessage extends Message {
    @Getter
    private String m_jarName;
    @Getter
    private byte[] m_jarBytes;

    public RegisterJarMessage() {
        super();
    }

    public RegisterJarMessage(final short p_destination, final String p_jarName, final byte[] p_jarBytes) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REGISTER);
        m_jarName = p_jarName;
        m_jarBytes = p_jarBytes;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += ObjectSizeUtil.sizeofString(m_jarName);
        size += ObjectSizeUtil.sizeofByteArray(m_jarBytes);

        return size;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_jarName);
        p_exporter.writeByteArray(m_jarBytes);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_jarName = p_importer.readString(m_jarName);
        m_jarBytes = p_importer.readByteArray(m_jarBytes);
    }
}
