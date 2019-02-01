package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkServiceRemoteCreateTest {
    @TestInstance(runOnNodeIdx = 2)
    public void createSize(final DXRAM p_instance) {
        createSize(p_instance, true, 1, 64, false);
        createSize(p_instance, false, 1, 64, false);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSize2(final DXRAM p_instance) {
        createSize(p_instance, true, 1, 1024 * 1024, false);
        createSize(p_instance, false, 1, 1024 * 1024, false);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSize3(final DXRAM p_instance) {
        createSize(p_instance, true, 1, 16 * 1024 * 1024, false);
        createSize(p_instance, false, 1, 16 * 1024 * 1024, false);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSize4(final DXRAM p_instance) {
        createSize(p_instance, true, 10, 16, false);
        createSize(p_instance, false, 10, 16, false);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSize5(final DXRAM p_instance) {
        createSize(p_instance, true, 100, 1, false);
        createSize(p_instance, false, 100, 1, false);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSize6(final DXRAM p_instance) {
        createSize(p_instance, true, 10, 16, true);
        createSize(p_instance, false, 10, 16, true);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSize7(final DXRAM p_instance) {
        createSize(p_instance, true, 100, 1, true);
        createSize(p_instance, false, 100, 1, true);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSizes(final DXRAM p_instance) {
        createSizes(p_instance, true, false, new int[] {64});
        createSizes(p_instance, false, false, new int[] {64});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSizes2(final DXRAM p_instance) {
        createSizes(p_instance, true, false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        createSizes(p_instance, false, false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSizes3(final DXRAM p_instance) {
        createSizes(p_instance, true, false, new int[] {32, 64, 128, 832, 99});
        createSizes(p_instance, false, false, new int[] {32, 64, 128, 832, 99});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSizes4(final DXRAM p_instance) {
        createSizes(p_instance, true, true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        createSizes(p_instance, false, true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSizes5(final DXRAM p_instance) {
        createSizes(p_instance, true, true, new int[] {32, 64, 128, 832, 99});
        createSizes(p_instance, false, true, new int[] {32, 64, 128, 832, 99});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSizes6(final DXRAM p_instance) {
        createSizes(p_instance, true, false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
        createSizes(p_instance, false, false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createSizes7(final DXRAM p_instance) {
        createSizes(p_instance, true, true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
        createSizes(p_instance, false, true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk(final DXRAM p_instance) {
        createChunk(p_instance, true, false, new int[] {64});
        createChunk(p_instance, false, false, new int[] {64});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk2(final DXRAM p_instance) {
        createChunk(p_instance, true, false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        createChunk(p_instance, false, false, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk3(final DXRAM p_instance) {
        createChunk(p_instance, true, false, new int[] {32, 64, 128, 832, 99});
        createChunk(p_instance, false, false, new int[] {32, 64, 128, 832, 99});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk4(final DXRAM p_instance) {
        createChunk(p_instance, true, true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        createChunk(p_instance, false, true, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk5(final DXRAM p_instance) {
        createChunk(p_instance, true, true, new int[] {32, 64, 128, 832, 99});
        createChunk(p_instance, false, true, new int[] {32, 64, 128, 832, 99});
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk6(final DXRAM p_instance) {
        createChunk2(p_instance, true, false, 1);
        createChunk2(p_instance, false, false, 1);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk7(final DXRAM p_instance) {
        createChunk2(p_instance, true, false, 20);
        createChunk2(p_instance, false, false, 20);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk8(final DXRAM p_instance) {
        createChunk2(p_instance, true, true, 20);
        createChunk2(p_instance, false, true, 20);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk9(final DXRAM p_instance) {
        createChunk(p_instance, true, false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
        createChunk(p_instance, false, false, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    @TestInstance(runOnNodeIdx = 2)
    public void createChunk10(final DXRAM p_instance) {
        createChunk(p_instance, true, true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
        createChunk(p_instance, false, true, ChunkTestUtils.generateRandomSizes(1000, 1, 1000));
    }

    private void createSize(final DXRAM p_instance, final boolean p_remote, final int p_count, final int p_size,
            final boolean p_consecutive) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        short remotePeer = p_remote ? ChunkTestUtils.getRemotePeer(p_instance) : getLocal(p_instance);

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

    private void createSizes(final DXRAM p_instance, final boolean p_remote, final boolean p_consecutive,
            final int[] p_sizes) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        short remotePeer = p_remote ? ChunkTestUtils.getRemotePeer(p_instance) : getLocal(p_instance);

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

    private void createChunk(final DXRAM p_instance, final boolean p_remote, final boolean p_consecutive,
            final int[] p_sizes) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        short remotePeer = p_remote ? ChunkTestUtils.getRemotePeer(p_instance) : getLocal(p_instance);

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

    private void createChunk2(final DXRAM p_instance, final boolean p_remote, final boolean p_consecutive,
            final int p_count) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        short remotePeer = p_remote ? ChunkTestUtils.getRemotePeer(p_instance) : getLocal(p_instance);

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

    private short getLocal(final DXRAM p_instance) {
        return p_instance.getService(BootService.class).getNodeID();
    }
}
