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

@SuppressWarnings("WeakerAccess")
public class MigrationTask implements Runnable {

    public enum Direction {
        IN, OUT
    }

    private final short m_target;

    private final long[] m_chunkIds;

    private final ChunkMigrator m_migrator;


    public MigrationTask(ChunkMigrator p_migrator, short p_target, long[] p_chunkIds) {

        m_migrator = p_migrator;

        m_target = p_target;

        m_chunkIds = p_chunkIds;
    }

    public int getChunkCount() {

        return m_chunkIds.length;
    }

    @Override
    public void run() {

        ChunkMigrator.Status status = m_migrator.migrate(m_chunkIds, m_target);

        m_migrator.onStatus(m_chunkIds, status);
    }
}
