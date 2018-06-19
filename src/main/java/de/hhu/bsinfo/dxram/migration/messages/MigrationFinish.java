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

package de.hhu.bsinfo.dxram.migration.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

public class MigrationFinish extends Message {

    private long[] m_chunkIds;

    private boolean m_status;

    public MigrationFinish(short p_destination, long[] p_chunkIds, boolean p_status) {

        super(p_destination, DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_FINISH);

        m_chunkIds = p_chunkIds;

        m_status = p_status;
    }

    @Override
    protected int getPayloadLength() {

        return Long.BYTES;
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {

        m_chunkIds = p_importer.readLongArray(m_chunkIds);

        m_status = p_importer.readBoolean(m_status);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {

        p_exporter.writeLongArray(m_chunkIds);

        p_exporter.writeBoolean(m_status);
    }

    public long[] getChunkIds() {
        return m_chunkIds;
    }

    public boolean isFinished() {
        return m_status;
    }
}
