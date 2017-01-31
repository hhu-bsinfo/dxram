/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.chunk;

import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.dxram.data.ChunkID;

public class ChunkIDRangeUtils {

    /**
     * Utils class
     */
    private ChunkIDRangeUtils() {

    }

    public static long getRandomChunkIdOfRanges(final List<Long> chunkIdRanges) {
        int rangeIdx = getRandomRangeExclEnd(0, chunkIdRanges.size() / 2);

        return getRandomChunkId(chunkIdRanges.get(rangeIdx * 2), chunkIdRanges.get(rangeIdx * 2 + 1));
    }

    public static long countTotalChunksOfRanges(final List<Long> p_ranges) {
        long count = 0;

        for (int i = 0; i < p_ranges.size(); i += 2) {
            long rangeStart = p_ranges.get(i);
            long rangeEnd = p_ranges.get(i + 1);

            count = rangeEnd - rangeStart + 1;
        }

        return count;
    }

    public static long[] distributeChunkCountsToThreads(final long p_chunkCount, final int p_threadCount) {
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

    public static ArrayList<Long>[] distributeChunkRangesToThreads(final long[] p_chunkCountsPerThread, final List<Long> p_ranges) {
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

                    rangeStart += chunkCount;
                    chunkCount = 0;
                } else {
                    // chunksInRange < chunkCount
                    distRanges[i].add(rangeStart);
                    distRanges[i].add(rangeEnd);

                    chunkCount -= chunksInRange;

                    rangeIdx++;
                    if (rangeIdx * 2 < p_ranges.size()) {
                        rangeStart = p_ranges.get(rangeIdx * 2);
                        rangeEnd = p_ranges.get(rangeIdx * 2 + 1);
                    } else {
                        break;
                    }
                }
            }
        }

        return distRanges;
    }

    public static long getRandomChunkId(final long p_start, final long p_end) {
        if (ChunkID.getCreatorID(p_start) != ChunkID.getCreatorID(p_end)) {
            return ChunkID.INVALID_ID;
        }

        return getRandomRange(p_start, p_end);
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
