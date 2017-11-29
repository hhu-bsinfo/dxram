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

package de.hhu.bsinfo.dxnet.core.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * This is a benchmark request which is used in DXNetMain.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 11.10.2017
 */
public class BenchmarkRequest extends Request {

    private byte[] m_data;

    /**
     * Creates an instance of BenchmarkRequest.
     */
    public BenchmarkRequest() {
        super();
    }

    /**
     * Creates an instance of BenchmarkRequest
     *
     * @param p_destination
     *         the destination nodeID
     */
    public BenchmarkRequest(final short p_destination, final int p_size) {
        super(p_destination, Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_REQUEST);

        m_data = new byte[p_size];
    }

    /**
     * Creates an instance of BenchmarkRequest
     *
     * @param p_destination
     *         the destination nodeID
     */
    public BenchmarkRequest(final short p_destination, final byte[] p_data) {
        super(p_destination, Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_REQUEST);

        m_data = p_data;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(m_data);
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_data);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_data = p_importer.readByteArray(m_data);
    }
}
