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

package de.hhu.bsinfo.dxram.migration;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public class MigrationTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MigrationTask.class);

    private final MigrationIdentifier m_identifier;

    private final long m_startId;

    private final long m_endId;

    private final ChunkMigrator m_migrator;

    public MigrationTask(ChunkMigrator p_migrator, MigrationIdentifier p_identifier, long p_startId, long p_endId) {

        m_migrator = p_migrator;

        m_identifier = p_identifier;

        m_startId = p_startId;

        m_endId = p_endId;
    }

    public int getChunkCount() {

        return (int) (m_endId - m_startId + 1);
    }

    @Override
    public void run() {

        log.debug("Starting Migration for Chunks [{} , {}]", ChunkID.toHexString(m_startId), ChunkID.toHexString(m_endId));

        ChunkMigrator.Status status = m_migrator.migrate(m_identifier, m_startId, m_endId);

        m_migrator.onStatus(m_identifier, m_startId, m_endId, status);
    }
}
