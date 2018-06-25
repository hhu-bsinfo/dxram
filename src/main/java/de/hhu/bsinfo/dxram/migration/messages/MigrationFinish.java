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
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class MigrationFinish extends Message {

    private MigrationIdentifier m_identifier;

    private long m_startId;

    private long m_endId;

    private boolean m_status;

    public MigrationFinish() {

        super();

        m_identifier = new MigrationIdentifier();
    }

    public MigrationFinish(MigrationIdentifier p_identifier, long p_startId, long p_endId, boolean p_status) {

        super(p_identifier.getSource(), DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_FINISH);

        m_identifier = p_identifier;

        m_startId = p_startId;

        m_endId = p_endId;

        m_status = p_status;
    }

    @Override
    protected int getPayloadLength() {

        return m_identifier.sizeofObject() + 2 * Long.BYTES + ObjectSizeUtil.sizeofBoolean();
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {

        p_importer.importObject(m_identifier);

        m_startId = p_importer.readLong(m_startId);

        m_endId = p_importer.readLong(m_endId);

        m_status = p_importer.readBoolean(m_status);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {

        p_exporter.exportObject(m_identifier);

        p_exporter.writeLong(m_startId);

        p_exporter.writeLong(m_endId);

        p_exporter.writeBoolean(m_status);
    }

    public MigrationIdentifier getIdentifier() {

        return m_identifier;
    }

    public long getEndId() {

        return m_endId;
    }

    public long getStartId() {

        return m_startId;
    }

    public boolean isFinished() {

        return m_status;
    }
}
