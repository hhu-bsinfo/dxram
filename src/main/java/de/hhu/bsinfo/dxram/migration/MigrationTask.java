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

import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@SuppressWarnings("WeakerAccess")
public class MigrationTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MigrationTask.class);

    private final MigrationIdentifier m_identifier;

    private final List<LongRange> m_ranges;

    private final ChunkMigrator m_migrator;

    public MigrationTask(ChunkMigrator p_migrator, MigrationIdentifier p_identifier, List<LongRange> p_ranges) {
        m_migrator = p_migrator;
        m_identifier = p_identifier;
        m_ranges = p_ranges;
    }

    public MigrationIdentifier getIdentifier() {
        return m_identifier;
    }

    public int getChunkCount() {
        return m_ranges.stream()
                .map(LongRange::size)
                .reduce(0, (a,b) -> a + b);
    }

    public List<LongRange> getRanges() {
        return m_ranges;
    }

    @Override
    public void run() {
        log.debug("Starting Migration {} for Chunks {}", m_identifier, LongRange.collectionToString(m_ranges));
        ChunkMigrator.Status status = m_migrator.migrate(m_identifier, m_ranges);
    }
}
