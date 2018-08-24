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
import de.hhu.bsinfo.dxram.migration.data.MigrationPayload;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;

public class MigrationPush extends Message {

    private MigrationIdentifier m_identifier;

    private MigrationPayload m_payload;

    public MigrationPush() {
        super();
        m_identifier = new MigrationIdentifier();
        m_payload = new MigrationPayload();
    }

    public MigrationPush(MigrationIdentifier p_identifier, MigrationPayload p_payload) {
        super(p_identifier.getTarget(), DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_PUSH);
        m_identifier = p_identifier;
        m_payload = p_payload;
    }

    @Override
    protected int getPayloadLength() {
        return m_identifier.sizeofObject() + m_payload.sizeofObject();
    }
    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        p_importer.importObject(m_identifier);
        p_importer.importObject(m_payload);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.exportObject(m_identifier);
        p_exporter.exportObject(m_payload);
    }

    public MigrationIdentifier getIdentifier() {
        return m_identifier;
    }

    public MigrationPayload getPayload() {
        return m_payload;
    }
}
