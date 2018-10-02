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

import javax.annotation.Nonnull;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Request to start an application on a remote peer
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.10.2018
 */
public class ApplicationStartRequest extends Request {
    private String m_name;
    private String[] m_args;

    /**
     * Creates an instance of ApplicationStartRequest.
     * This constructor is used when receiving this message.
     */
    public ApplicationStartRequest() {
        super();
    }

    /**
     * Creates an instance of ApplicationStartRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_name
     *         Name of the application
     * @param p_args
     *         Arguments for the application
     */
    public ApplicationStartRequest(final short p_destination, @Nonnull final String p_name,
            @Nonnull final String[] p_args) {
        super(p_destination, DXRAMMessageTypes.APPLICATION_MESSAGE_TYPE,
                ApplicationMessages.SUBTYPE_START_APPLICATION_REQUEST);

        m_name = p_name;
        m_args = p_args;
    }

    /**
     * Get the name of the application to run
     *
     * @return Name of application
     */
    public String getName() {
        return m_name;
    }

    /**
     * Get the arguments for the application to run
     *
     * @return Arguments
     */
    public String[] getArgs() {
        return m_args;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_name);
        p_exporter.writeInt(m_args.length);

        for (int i = 0; i < m_args.length; i++) {
            p_exporter.writeString(m_args[i]);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_name = p_importer.readString(m_name);

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

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += ObjectSizeUtil.sizeofString(m_name);
        size += Integer.BYTES;

        for (String str : m_args) {
            size += ObjectSizeUtil.sizeofString(str);
        }

        return size;
    }
}
