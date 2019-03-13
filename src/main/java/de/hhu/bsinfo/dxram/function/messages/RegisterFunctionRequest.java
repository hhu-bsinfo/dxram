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

package de.hhu.bsinfo.dxram.function.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.function.DistributableFunction;
import de.hhu.bsinfo.dxram.function.util.FunctionSerializer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class RegisterFunctionRequest extends Request {

    private DistributableFunction m_function;
    private byte[] m_functionBytes;
    private String m_name;

    public RegisterFunctionRequest() {
        super();
    }

    public RegisterFunctionRequest(final short p_destination, final DistributableFunction p_function, final String p_name) {
        super(p_destination, DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_REGISTER_FUNCTION_REQUEST);

        m_function = p_function;
        m_name = p_name;
        m_functionBytes = FunctionSerializer.serialize(p_function);
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_functionBytes);
        p_exporter.writeString(m_name);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_functionBytes = p_importer.readByteArray(m_functionBytes);
        m_name = p_importer.readString(m_name);
    }

    public DistributableFunction getFunction() {
        if (m_function == null) {
            m_function = FunctionSerializer.deserialize(m_functionBytes);
        }

        return m_function;
    }

    public String getName() {
        return m_name;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(m_functionBytes) + ObjectSizeUtil.sizeofString(m_name);
    }
}
