package de.hhu.bsinfo.dxcompute.bench;

import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.unit.StorageUnit;

final class ChunkTaskUtils {

    private ChunkTaskUtils() {

    }

    static short getSuccessorSlaveNodeId(final short[] p_slaveNodeIds, final short p_ownSlaveId) {
        if (p_ownSlaveId + 1 < p_slaveNodeIds.length) {
            return p_slaveNodeIds[p_ownSlaveId + 1];
        } else {
            return p_slaveNodeIds[0];
        }
    }

    static long[] distributeChunkCountsToThreads(final long p_chunkCount, final int p_threadCount) {
        long count = p_chunkCount;
        long[] chunkCounts = new long[p_threadCount];

        for (int i = 0; i < chunkCounts.length; i++) {
            chunkCounts[i] = p_chunkCount / p_threadCount;
            count -= chunkCounts[i];
        }

        for (int i = 0; i < count; i++) {
            chunkCounts[i]++;
        }

        return chunkCounts;
    }

    static ArrayList<Long>[] distributeChunkRangesToThreads(final long[] p_chunkCountsPerThread, final List<Long> p_ranges) {
        ArrayList<Long>[] distRanges = new ArrayList[p_chunkCountsPerThread.length];
        for (int i = 0; i < distRanges.length; i++) {
            distRanges[i] = new ArrayList<>();
        }

        int rangeIdx = 0;
        long rangeStart = p_ranges.get(rangeIdx * 2);
        long rangeEnd = p_ranges.get(rangeIdx * 2 + 1);

        for (int i = 0; i < p_chunkCountsPerThread.length; i++) {
            long chunkCount = p_chunkCountsPerThread[i];

            while (chunkCount > 0) {
                long chunksInRange = ChunkID.getLocalID(rangeEnd) - ChunkID.getLocalID(rangeStart) + 1;
                if (chunksInRange >= chunkCount) {
                    distRanges[i].add(rangeStart);
                    distRanges[i].add(rangeStart + chunkCount - 1);

                    rangeStart = rangeStart + chunkCount;
                    chunkCount = 0;
                } else {
                    // chunksInRange < chunkCount
                    distRanges[i].add(rangeStart);
                    distRanges[i].add(rangeEnd);

                    chunkCount -= chunksInRange;

                    rangeIdx++;
                    rangeStart = p_ranges.get(rangeIdx * 2);
                    rangeEnd = p_ranges.get(rangeIdx * 2 + 1);
                }
            }
        }

        return distRanges;
    }

    static int getRandomSize(final StorageUnit p_start, final StorageUnit p_end) {
        return (int) getRandomRange(p_start.getBytes(), p_end.getBytes());
    }

    static long getRandomChunkId(final long p_start, final long p_end) {
        if (ChunkID.getCreatorID(p_start) != ChunkID.getCreatorID(p_end)) {
            return ChunkID.INVALID_ID;
        }

        return getRandomRange(p_start, p_end);
    }

    static long getRandomChunkIdOfRanges(final List<Long> chunkIdRanges) {
        int rangeIdx = getRandomRangeExclEnd(0, chunkIdRanges.size() / 2);

        return getRandomChunkId(chunkIdRanges.get(rangeIdx * 2), chunkIdRanges.get(rangeIdx * 2 + 1));
    }

    private static int getRandomRange(final int p_start, final int p_end) {
        return (int) (Math.random() * (p_end - p_start + 1) + p_start);
    }

    private static int getRandomRangeExclEnd(final int p_start, final int p_end) {
        return (int) (Math.random() * (p_end - p_start) + p_start);
    }

    private static long getRandomRange(final long p_start, final long p_end) {
        return (long) (Math.random() * (p_end - p_start + 1) + p_start);
    }
}
