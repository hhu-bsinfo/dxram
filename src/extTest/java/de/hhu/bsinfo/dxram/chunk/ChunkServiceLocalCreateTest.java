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

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(runTestOnNodeIdx = 1,
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class ChunkServiceLocalCreateTest {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void createSize() {
        createSize(1, 64, false);
    }

    @Test
    public void createSize2() {
        createSize(1, 1024 * 1024, false);
    }

    @Test
    public void createSize3() {
        createSize(1, 16 * 1024 * 1024, false);
    }

    @Test
    public void createSize4() {
        createSize(10, 16, false);
    }

    @Test
    public void createSize5() {
        createSize(100, 1, false);
    }

    @Test
    public void createSize6() {
        createSize(10, 16, true);
    }

    @Test
    public void createSize7() {
        createSize(100, 1, true);
    }

    @Test
    public void createSizes() {
        createSizes(false, new int[] {64});
    }

    @Test
    public void createSizes2() {
        createSizes(false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @Test
    public void createSizes3() {
        createSizes(false, new int[] {32, 64, 128, 832, 99});
    }

    @Test
    public void createSizes4() {
        createSizes(true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @Test
    public void createSizes5() {
        createSizes(true, new int[] {32, 64, 128, 832, 99});
    }

    @Test
    public void createSizes6() {
        createSizes(false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    @Test
    public void createSizes7() {
        createSizes(true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    @Test
    public void createChunk() {
        createChunk(false, new int[] {64});
    }

    @Test
    public void createChunk2() {
        createChunk(false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @Test
    public void createChunk3() {
        createChunk(false, new int[] {32, 64, 128, 832, 99});
    }

    @Test
    public void createChunk4() {
        createChunk(true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @Test
    public void createChunk5() {
        createChunk(true, new int[] {32, 64, 128, 832, 99});
    }

    @Test
    public void createChunk6() {
        createChunk2(false, 1);
    }

    @Test
    public void createChunk7() {
        createChunk2(false, 20);
    }

    @Test
    public void createChunk8() {
        createChunk2(true, 20);
    }

    @Test
    public void createChunk9() {
        createChunk(false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    @Test
    public void createChunk10() {
        createChunk(true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    private void createSize(final int p_count, final int p_size, final boolean p_consecutive) {
        ChunkLocalService chunkLocalService = m_instance.getService(ChunkLocalService.class);

        long[] cids = new long[p_count];
        int created = chunkLocalService.createLocal().create(cids, 0, p_count, p_size, p_consecutive);

        Assert.assertEquals(p_count, created);

        long start = ChunkID.INVALID_ID;

        for (int i = 0; i < p_count; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);

            if (p_consecutive) {
                if (start == ChunkID.INVALID_ID) {
                    start = cids[i];
                }

                Assert.assertEquals(start + i, cids[i]);
            }
        }
    }

    private void createSizes(final boolean p_consecutive, final int[] p_sizes) {
        ChunkLocalService chunkLocalService = m_instance.getService(ChunkLocalService.class);

        long[] cids = new long[p_sizes.length];
        int created = chunkLocalService.createLocal().createSizes(cids, p_consecutive, p_sizes);

        Assert.assertEquals(p_sizes.length, created);

        long start = ChunkID.INVALID_ID;

        for (int i = 0; i < p_sizes.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);

            if (p_consecutive) {
                if (start == ChunkID.INVALID_ID) {
                    start = cids[i];
                }

                Assert.assertEquals(start + i, cids[i]);
            }
        }
    }

    private void createChunk(final boolean p_consecutive, final int[] p_sizes) {
        ChunkLocalService chunkLocalService = m_instance.getService(ChunkLocalService.class);

        ChunkByteArray[] chunks = new ChunkByteArray[p_sizes.length];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new ChunkByteArray(p_sizes[i]);
        }

        int created = chunkLocalService.createLocal().create(p_consecutive, chunks);

        Assert.assertEquals(p_sizes.length, created);

        long start = ChunkID.INVALID_ID;

        for (int i = 0; i < p_sizes.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, chunks[i].getID());
            Assert.assertTrue(chunks[i].isStateOk());

            if (p_consecutive) {
                if (start == ChunkID.INVALID_ID) {
                    start = chunks[i].getID();
                }

                Assert.assertEquals(start + i, chunks[i].getID());
            }
        }
    }

    private void createChunk2(final boolean p_consecutive, final int p_count) {
        ChunkLocalService chunkLocalService = m_instance.getService(ChunkLocalService.class);

        TestChunk[] chunks = new TestChunk[p_count];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new TestChunk(false);
        }

        int created = chunkLocalService.createLocal().create(p_consecutive, chunks);

        Assert.assertEquals(p_count, created);

        long start = ChunkID.INVALID_ID;

        for (int i = 0; i < p_count; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, chunks[i].getID());
            Assert.assertTrue(chunks[i].isStateOk());

            if (p_consecutive) {
                if (start == ChunkID.INVALID_ID) {
                    start = chunks[i].getID();
                }

                Assert.assertEquals(start + i, chunks[i].getID());
            }
        }
    }
}
