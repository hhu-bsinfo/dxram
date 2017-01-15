package de.hhu.bsinfo.dxcompute.bench;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import java.util.List;

final class ChunkTaskUtils {

    private ChunkTaskUtils() {

    }

    static int[] distributeChunkCountsToThreads(final int p_chunkCount, final int p_threadCount) {
        int count = p_chunkCount;
        int[] chunkCounts = new int[p_threadCount];

        for (int i = 0; i < chunkCounts.length; i++) {
            chunkCounts[i] = p_chunkCount / p_threadCount;
            count -= chunkCounts[i];
        }

        for (int i = 0; i < count; i++) {
            chunkCounts[i]++;
        }

        return chunkCounts;
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

        return getRandomChunkId(chunkIdRanges.get(rangeIdx), chunkIdRanges.get(rangeIdx + 1));
    }

    private static int getRandomRange(final int p_start, final int p_end) {
        return (int) (Math.random() * (p_end - p_start + 1) + p_start);
    }

    private static int getRandomRangeExclEnd(final int p_start, final int p_end) {
        return (int) (Math.random() * (p_end - p_start) + p_start);
    }

    private static long getRandomRange(final long p_start, final long p_end) {
        return (int) (Math.random() * (p_end - p_start + 1) + p_start);
    }
}
