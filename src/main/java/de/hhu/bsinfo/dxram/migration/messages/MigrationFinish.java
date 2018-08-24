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
import de.hhu.bsinfo.dxram.migration.LongRange;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Collection;
import java.util.List;

public class MigrationFinish extends Message {

    private MigrationIdentifier m_identifier;

    private long[] m_ranges;

    private boolean m_status;

    public MigrationFinish() {
        super();
        m_identifier = new MigrationIdentifier();
    }

    public MigrationFinish(MigrationIdentifier p_identifier, Collection<LongRange> p_ranges, boolean p_status) {
        super(p_identifier.getSource(), DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_FINISH);
        m_identifier = p_identifier;
        m_ranges = LongRange.collectionToArray(p_ranges);
        m_status = p_status;
    }

    @Override
    protected int getPayloadLength() {
        return m_identifier.sizeofObject() + ObjectSizeUtil.sizeofLongArray(m_ranges) + ObjectSizeUtil.sizeofBoolean();
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        p_importer.importObject(m_identifier);
        m_ranges = p_importer.readLongArray(m_ranges);
        m_status = p_importer.readBoolean(m_status);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.exportObject(m_identifier);
        p_exporter.writeLongArray(m_ranges);
        p_exporter.writeBoolean(m_status);
    }

    public MigrationIdentifier getIdentifier() {
        return m_identifier;
    }

    public long[] getRanges() {
        return m_ranges;
    }

    public Collection<LongRange> getLongRanges() {
        return LongRange.collectionFromArray(m_ranges);
    }

    public boolean isFinished() {
        return m_status;
    }
}
