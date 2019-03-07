/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
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

package de.hhu.bsinfo.dxram.app.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.messages.BootMessages;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Used for submitting DXRAM applications.
 *
 * @author Filip Krakowski, filip.krakowski@uni-duesseldorf.de, 06.03.2019
 */
public class ApplicationSubmitMessage extends Message {

    private String m_archiveName;
    private String[] m_args;

    public ApplicationSubmitMessage() {
        super();
    }

    public ApplicationSubmitMessage(final short p_destination, final String p_archiveName, final String[] p_args) {
        super(p_destination, DXRAMMessageTypes.APPLICATION_MESSAGE_TYPE, ApplicationMessages.SUBTYPE_SUBMIT_APPLICATION);
        m_archiveName = p_archiveName;
        m_args = p_args;
    }

    public String getArchiveName() {
        return m_archiveName;
    }

    public String[] getArgs() {
        return m_args;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += ObjectSizeUtil.sizeofString(m_archiveName);
        size += Integer.BYTES;

        for (String str : m_args) {
            size += ObjectSizeUtil.sizeofString(str);
        }

        return size;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_archiveName);
        p_exporter.writeInt(m_args.length);

        for (int i = 0; i < m_args.length; i++) {
            p_exporter.writeString(m_args[i]);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_archiveName = p_importer.readString(m_archiveName);

        int size = p_importer.readInt(0);

        if (m_args == null) {
            m_args = new String[size];

            for (int i = 0; i < m_args.length; i++) {
                m_args[i] = "";
            }
        }

        for (int i = 0; i < m_args.length; i++) {
            m_args[i] = p_importer.readString(m_args[i]);
        }
    }
}
