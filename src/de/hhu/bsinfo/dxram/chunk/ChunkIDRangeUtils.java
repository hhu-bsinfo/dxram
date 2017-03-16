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

import java.util.List;

import de.hhu.bsinfo.dxram.data.ChunkID;

public class ChunkIDRangeUtils {

    /**
     * Utils class
     */
    private ChunkIDRangeUtils() {

    }

    public static boolean isInRanges(final long p_chunkID, final List<Long> p_chunkIdRanges) {
        for (int i = 0; i < p_chunkIdRanges.size(); i += 2) {
            if (p_chunkIdRanges.get(i) <= p_chunkID && p_chunkID <= p_chunkIdRanges.get(i + 1)) {
                return true;
            }
        }

        return false;
    }

    public static long getRandomChunkIdOfRanges(final List<Long> p_chunkIdRanges) {
        int rangeIdx = getRandomRangeExclEnd(0, p_chunkIdRanges.size() / 2);

        return getRandomChunkId(p_chunkIdRanges.get(rangeIdx * 2), p_chunkIdRanges.get(rangeIdx * 2 + 1));
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

    public static long getRandomChunkId(final long p_start, final long p_end) {
        if (ChunkID.getCreatorID(p_start) != ChunkID.getCreatorID(p_end)) {
            return ChunkID.INVALID_ID;
        }

        return getRandomRange(p_start, p_end);
    }

    private static int getRandomRange(final int p_start, final int p_end) {
        if (p_start == p_end) {
            return p_start;
        }

        return (int) (Math.random() * (p_end - p_start + 1) + p_start);
    }

    private static int getRandomRangeExclEnd(final int p_start, final int p_end) {
        return (int) (Math.random() * (p_end - p_start) + p_start);
    }

    private static long getRandomRange(final long p_start, final long p_end) {
        if (p_start == p_end) {
            return p_start;
        }

        long tmp = (long) (Math.random() * (p_end - p_start + 1));
        return tmp + p_start;
    }
}
