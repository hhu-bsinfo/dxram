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

package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMRunnerConfiguration;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMRunnerConfiguration(runTestOnNodeIdx = 1,
        nodes = {
                @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class ChunkServiceLocalCreateTest {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void create() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        long[] chunkIds = chunkService.create(64, 1);

        Assert.assertEquals(chunkIds.length, 1);
        Assert.assertNotEquals(chunkIds[0], ChunkID.INVALID_ID);
    }

    @Test
    public void create2() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        long[] chunkIds = chunkService.create(64, 10);

        Assert.assertEquals(chunkIds.length, 10);

        for (int i = 0; i < chunkIds.length; i++) {
            Assert.assertNotEquals(chunkIds[i], ChunkID.INVALID_ID);
        }
    }

    @Test
    public void create3() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        long[] chunkIds = chunkService.create(256, 10, true);

        Assert.assertEquals(chunkIds.length, 10);

        long start = ChunkID.INVALID_ID;

        for (int i = 0; i < chunkIds.length; i++) {
            if (start == ChunkID.INVALID_ID) {
                start = chunkIds[i];
            }

            Assert.assertNotEquals(chunkIds[i], ChunkID.INVALID_ID);
            Assert.assertEquals(start + i, chunkIds[i]);
        }
    }

    @Test
    public void create4() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);

        TestChunk chunk = new TestChunk(true);
        chunkService.create(chunk);

        Assert.assertNotEquals(ChunkID.INVALID_ID, chunk.getID());
        Assert.assertEquals(ChunkState.OK, chunk.getState());
    }

    @Test
    public void create5() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);

        TestChunk[] chunks = new TestChunk[15];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new TestChunk(false);
        }

        chunkService.create(chunks);

        for (TestChunk chunk : chunks) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, chunk.getID());
            Assert.assertEquals(ChunkState.OK, chunk.getState());
        }
    }

    @Test
    public void create6() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);

        TestChunk[] chunks = new TestChunk[15];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new TestChunk(false);
        }

        chunkService.create(chunks);

        long start = ChunkID.INVALID_ID;

        for (int i = 0; i < chunks.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, chunks[i].getID());
            Assert.assertEquals(ChunkState.OK, chunks[i].getState());

            if (start == ChunkID.INVALID_ID) {
                start = chunks[i].getID();
            }

            Assert.assertEquals(start + i, chunks[i].getID());
        }
    }

    @Test
    public void createSizes() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);

        int[] sizes = new int[] {64, 128, 321, 932, 999};
        long[] ids = chunkService.createSizes(sizes);

        for (int i = 0; i < sizes.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, ids[i]);
        }
    }

    @Test
    public void createSizes2() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);

        int[] sizes = new int[] {64, 128, 321, 932, 999};
        long[] ids = chunkService.createSizes(true, sizes);

        long start = ChunkID.INVALID_ID;

        for (int i = 0; i < sizes.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, ids[i]);

            if (start == ChunkID.INVALID_ID) {
                start = ids[i];
            }

            Assert.assertEquals(start + i, ids[i]);
        }
    }
}
