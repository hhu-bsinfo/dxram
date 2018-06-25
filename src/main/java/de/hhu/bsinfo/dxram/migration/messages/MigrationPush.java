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
import de.hhu.bsinfo.dxram.migration.data.ChunkRange;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;

public class MigrationPush extends Message {

    private MigrationIdentifier m_identifier;

    private ChunkRange m_data;

    public MigrationPush() {

        super();

        m_identifier = new MigrationIdentifier();

        m_data = new ChunkRange();
    }

    public MigrationPush(MigrationIdentifier p_identifier, ChunkRange p_data) {

        super(p_identifier.getTarget(), DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_PUSH);

        m_identifier = p_identifier;

        m_data = p_data;
    }

    @Override
    protected int getPayloadLength() {

        return m_identifier.sizeofObject() + m_data.sizeofObject();
    }
    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {

        p_importer.importObject(m_identifier);

        p_importer.importObject(m_data);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {

        p_exporter.exportObject(m_identifier);

        p_exporter.exportObject(m_data);
    }

    public MigrationIdentifier getIdentifier() {

        return m_identifier;
    }

    public ChunkRange getChunkRange() {

        return m_data;
    }
}
