package de.hhu.bsinfo.dxram.chunk;

import java.util.List;

import org.junit.Assert;

import de.hhu.bsinfo.dxmem.data.ChunkBenchmark;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxutils.RandomUtils;

public class ChunkTestUtils {
    private ChunkTestUtils() {

    }

    public static void createChunks(final DXRAM p_dxram, final ChunkIDRanges p_ranges, final boolean p_remote,
            final int p_size, final int p_count) {
        ChunkService chunkService = p_dxram.getService(ChunkService.class);
        short peer = p_remote ? ChunkTestUtils.getRemotePeer(p_dxram) : getLocalPeer(p_dxram);

        ChunkByteArray chunk = new ChunkByteArray(p_size);

        for (int i = 0; i < p_count; i++) {
            int ret = chunkService.create().create(peer, chunk);

            Assert.assertEquals(1, ret);
            Assert.assertTrue(chunk.isIDValid());
            Assert.assertTrue(chunk.isStateOk());
            Assert.assertEquals(peer, ChunkID.getCreatorID(chunk.getID()));

            p_ranges.add(chunk.getID());
        }
    }

    public static void createChunksMulti(final DXRAM p_dxram, final ChunkIDRanges p_ranges, final boolean p_remote,
            final int p_size, final int p_count) {
        ChunkService chunkService = p_dxram.getService(ChunkService.class);
        short peer = p_remote ? ChunkTestUtils.getRemotePeer(p_dxram) : getLocalPeer(p_dxram);

        long[] cids = new long[p_count];

        int ret = chunkService.create().create(peer, cids, p_count, p_size);

        Assert.assertEquals(p_count, ret);

        for (int i = 0; i < p_count; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
            Assert.assertEquals(peer, ChunkID.getCreatorID(cids[i]));

            p_ranges.add(cids[i]);
        }
    }

    public static void putAndGetAll(final DXRAM p_dxram, final ChunkIDRanges p_ranges, final int p_size) {
        ChunkService chunkService = p_dxram.getService(ChunkService.class);

        ChunkBenchmark chunk = new ChunkBenchmark(p_size);

        for (long cid : p_ranges) {
            chunk.setID(cid);
            chunk.fillContents();
            Assert.assertEquals(1, chunkService.put().put(chunk));
            Assert.assertTrue(chunk.isStateOk());

            Assert.assertEquals(1, chunkService.get().get(chunk));
            Assert.assertTrue(chunk.isStateOk());
            Assert.assertTrue(chunk.verifyContents());
        }
    }

    public static void putAndGetLockAll(final DXRAM p_dxram, final ChunkIDRanges p_ranges, final int p_size) {
        ChunkService chunkService = p_dxram.getService(ChunkService.class);

        ChunkBenchmark chunk = new ChunkBenchmark(p_size);

        for (long cid : p_ranges) {
            chunk.setID(cid);
            Assert.assertEquals(1, chunkService.get().get(ChunkLockOperation.ACQUIRE_BEFORE_OP, 1000, chunk));
            Assert.assertTrue(chunk.isStateOk());

            chunk.fillContents();
            Assert.assertEquals(1, chunkService.put().put(ChunkLockOperation.RELEASE_AFTER_OP, 1000, chunk));
            Assert.assertTrue(chunk.isStateOk());

            Assert.assertEquals(1, chunkService.get().get(ChunkLockOperation.ACQUIRE_OP_RELEASE, 1000, chunk));
            Assert.assertTrue(chunk.isStateOk());
            Assert.assertTrue(chunk.verifyContents());
        }
    }

    public static void putAndGetMultiAll(final DXRAM p_dxram, final ChunkIDRanges p_ranges, final int p_size,
            final int p_batchSize) {
        ChunkService chunkService = p_dxram.getService(ChunkService.class);

        ChunkBenchmark[] chunks = new ChunkBenchmark[p_batchSize];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new ChunkBenchmark(p_size);
        }

        int i = 0;

        for (long cid : p_ranges) {
            if (i < chunks.length) {
                chunks[i].setID(cid);
                chunks[i].fillContents();
                i++;
            } else {
                Assert.assertEquals(i, chunkService.put().put(chunks));

                for (ChunkBenchmark chunk : chunks) {
                    Assert.assertTrue(chunk.isStateOk());
                }

                Assert.assertEquals(i, chunkService.get().get(chunks));

                for (ChunkBenchmark chunk : chunks) {
                    Assert.assertTrue(chunk.isStateOk());
                    Assert.assertTrue(chunk.verifyContents());
                }

                i = 0;
            }
        }

        if (i > 0) {
            Assert.assertEquals(i, chunkService.put().put(0, i, ChunkLockOperation.NONE, -1, chunks));

            for (int j = 0; j < i; j++) {
                Assert.assertTrue(chunks[j].isStateOk());
            }

            Assert.assertEquals(i, chunkService.get().get(0, i, ChunkLockOperation.NONE, -1, chunks));

            for (int j = 0; j < i; j++) {
                Assert.assertTrue(chunks[j].isStateOk());
                Assert.assertTrue(chunks[j].verifyContents());
            }
        }
    }

    public static int[] generateRandomSizes(final int p_count, final int p_start, final int p_end) {
        int[] sizes = new int[p_count];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = RandomUtils.getRandomValue(p_start, p_end);
        }

        return sizes;
    }

    public static ChunkByteArray[] generateRandomChunks(final int p_count, final int p_start, final int p_end) {
        ChunkByteArray[] chunks = new ChunkByteArray[p_count];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new ChunkByteArray(RandomUtils.getRandomValue(p_start, p_end));
        }

        return chunks;
    }

    public static short getRemotePeer(final DXRAM p_instance) {
        BootService bootService = p_instance.getService(BootService.class);

        List<Short> peers = bootService.getOnlinePeerNodeIDs();

        for (short peer : peers) {
            if (peer != bootService.getNodeID()) {
                return peer;
            }
        }

        throw new IllegalStateException("Could not find other peer");
    }

    private static short getLocalPeer(final DXRAM p_instance) {
        return p_instance.getService(BootService.class).getNodeID();
    }
}
