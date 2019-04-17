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
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class ClassResponseMessage extends Response {
    @Getter
    private byte[] m_jarBytes;
    @Getter
    private String m_jarName;

    public ClassResponseMessage() {
        super();
    }

    public ClassResponseMessage(final ClassRequestMessage p_request, final String p_jarName, final byte[] p_jarBytes) {
        super(p_request, LoaderMessages.SUBTYPE_CLASS_RESPONSE);
        m_jarName = p_jarName;
        m_jarBytes = p_jarBytes;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += ObjectSizeUtil.sizeofByteArray(m_jarBytes);
        size += ObjectSizeUtil.sizeofString(m_jarName);

        return size;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_jarBytes);
        p_exporter.writeString(m_jarName);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_jarBytes = p_importer.readByteArray(m_jarBytes);
        m_jarName = p_importer.readString(m_jarName);
    }
}
