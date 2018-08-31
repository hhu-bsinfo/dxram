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
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(runTestOnNodeIdx = 2,
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkServiceRemoteCreateTest {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void createSize() {
        createSize(true, 1, 64, false);
        createSize(false, 1, 64, false);
    }

    @Test
    public void createSize2() {
        createSize(true, 1, 1024 * 1024, false);
        createSize(false, 1, 1024 * 1024, false);
    }

    @Test
    public void createSize3() {
        createSize(true, 1, 16 * 1024 * 1024, false);
        createSize(false, 1, 16 * 1024 * 1024, false);
    }

    @Test
    public void createSize4() {
        createSize(true, 10, 16, false);
        createSize(false, 10, 16, false);
    }

    @Test
    public void createSize5() {
        createSize(true, 100, 1, false);
        createSize(false, 100, 1, false);
    }

    @Test
    public void createSize6() {
        createSize(true, 10, 16, true);
        createSize(false, 10, 16, true);
    }

    @Test
    public void createSize7() {
        createSize(true, 100, 1, true);
        createSize(false, 100, 1, true);
    }

    @Test
    public void createSizes() {
        createSizes(true, false, new int[] {64});
        createSizes(false, false, new int[] {64});
    }

    @Test
    public void createSizes2() {
        createSizes(true, false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        createSizes(false, false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @Test
    public void createSizes3() {
        createSizes(true, false, new int[] {32, 64, 128, 832, 99});
        createSizes(false, false, new int[] {32, 64, 128, 832, 99});
    }

    @Test
    public void createSizes4() {
        createSizes(true, true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        createSizes(false, true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @Test
    public void createSizes5() {
        createSizes(true, true, new int[] {32, 64, 128, 832, 99});
        createSizes(false, true, new int[] {32, 64, 128, 832, 99});
    }

    @Test
    public void createSizes6() {
        createSizes(true, false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
        createSizes(false, false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    @Test
    public void createSizes7() {
        createSizes(true, true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
        createSizes(false, true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    @Test
    public void createChunk() {
        createChunk(true, false, new int[] {64});
        createChunk(false, false, new int[] {64});
    }

    @Test
    public void createChunk2() {
        createChunk(true, false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        createChunk(false, false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @Test
    public void createChunk3() {
        createChunk(true, false, new int[] {32, 64, 128, 832, 99});
        createChunk(false, false, new int[] {32, 64, 128, 832, 99});
    }

    @Test
    public void createChunk4() {
        createChunk(true, true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        createChunk(false, true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @Test
    public void createChunk5() {
        createChunk(true, true, new int[] {32, 64, 128, 832, 99});
        createChunk(false, true, new int[] {32, 64, 128, 832, 99});
    }

    @Test
    public void createChunk6() {
        createChunk2(true, false, 1);
        createChunk2(false, false, 1);
    }

    @Test
    public void createChunk7() {
        createChunk2(true, false, 20);
        createChunk2(false, false, 20);
    }

    @Test
    public void createChunk8() {
        createChunk2(true, true, 20);
        createChunk2(false, true, 20);
    }

    @Test
    public void createChunk9() {
        createChunk(true, false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
        createChunk(false, false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    @Test
    public void createChunk10() {
        createChunk(true, true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
        createChunk(false, true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    private void createSize(final boolean p_remote, final int p_count, final int p_size, final boolean p_consecutive) {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        short remotePeer = p_remote ? ChunkTestUtils.getRemotePeer(m_instance) : getLocal();

        long[] cids = new long[p_count];
        int created = chunkService.create().create(remotePeer, cids, 0, p_count, p_size, p_consecutive);

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

    private void createSizes(final boolean p_remote, final boolean p_consecutive, final int[] p_sizes) {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        short remotePeer = p_remote ? ChunkTestUtils.getRemotePeer(m_instance) : getLocal();

        long[] cids = new long[p_sizes.length];
        int created = chunkService.create().createSizes(remotePeer, cids, p_consecutive, p_sizes);

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

    private void createChunk(final boolean p_remote, final boolean p_consecutive, final int[] p_sizes) {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        short remotePeer = p_remote ? ChunkTestUtils.getRemotePeer(m_instance) : getLocal();

        ChunkByteArray[] chunks = new ChunkByteArray[p_sizes.length];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new ChunkByteArray(p_sizes[i]);
        }

        int created = chunkService.create().create(remotePeer, p_consecutive, chunks);

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

    private void createChunk2(final boolean p_remote, final boolean p_consecutive, final int p_count) {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        short remotePeer = p_remote ? ChunkTestUtils.getRemotePeer(m_instance) : getLocal();

        TestChunk[] chunks = new TestChunk[p_count];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new TestChunk(false);
        }

        int created = chunkService.create().create(remotePeer, p_consecutive, chunks);

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

    private short getLocal() {
        return m_instance.getService(BootService.class).getNodeID();
    }
}
