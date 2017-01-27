package de.hhu.bsinfo.dxcompute.bench;

import java.util.ArrayList;

import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.unit.StorageUnit;

final class ChunkTaskUtils {
    public static final int PATTERN_LOCAL_ONLY = 0;
    public static final int PATTERN_REMOTE_ONLY_SUCCESSOR = 1;
    public static final int PATTERN_REMOTE_ONLY_RANDOM = 2;
    public static final int PATTERN_REMOTE_LOCAL_MIXED_RANDOM = 3;

    private ChunkTaskUtils() {

    }

    static ArrayList<Long> getChunkRangesForTestPattern(final int p_pattern, final TaskContext p_ctx, final ChunkService p_chunkService) {
        ArrayList<Long> allChunkRanges;

        switch (p_pattern) {
            case PATTERN_LOCAL_ONLY:
                allChunkRanges = p_chunkService.getAllLocalChunkIDRanges();
                break;

            case PATTERN_REMOTE_ONLY_SUCCESSOR:
                short slaveId = p_ctx.getCtxData().getSlaveId();
                short successorSlaveId = (short) ((slaveId + 1) % p_ctx.getCtxData().getSlaveNodeIds().length);

                allChunkRanges = p_chunkService.getAllLocalChunkIDRanges(p_ctx.getCtxData().getSlaveNodeIds()[successorSlaveId]);

                break;

            case PATTERN_REMOTE_ONLY_RANDOM:
                short ownNodeId = p_ctx.getCtxData().getOwnNodeId();

                allChunkRanges = new ArrayList<>();
                for (int i = 0; i < p_ctx.getCtxData().getSlaveNodeIds().length; i++) {
                    if (p_ctx.getCtxData().getSlaveNodeIds()[i] != ownNodeId) {
                        allChunkRanges.addAll(p_chunkService.getAllLocalChunkIDRanges(p_ctx.getCtxData().getSlaveNodeIds()[i]));
                    }
                }

                break;

            case PATTERN_REMOTE_LOCAL_MIXED_RANDOM:
                allChunkRanges = new ArrayList<>();
                for (int i = 0; i < p_ctx.getCtxData().getSlaveNodeIds().length; i++) {
                    allChunkRanges.addAll(p_chunkService.getAllLocalChunkIDRanges(p_ctx.getCtxData().getSlaveNodeIds()[i]));
                }

                break;

            default:
                System.out.println("Unknown pattern " + p_pattern);
                return null;
        }

        // modify ranges to avoid deleting an index chunk
        for (int i = 0; i < allChunkRanges.size(); i += 2) {
            long rangeStart = allChunkRanges.get(i);
            if (ChunkID.getLocalID(rangeStart) == 0) {
                allChunkRanges.set(i, rangeStart + 1);
            }
        }

        return allChunkRanges;
    }

    static short getRandomNodeIdExceptOwn(final short[] p_slaveNodeIds, final short p_ownNodeId) {
        short nodeId = p_ownNodeId;

        while (nodeId == p_ownNodeId) {
            nodeId = getRandomNodeId(p_slaveNodeIds);
        }

        return nodeId;
    }

    static short getRandomNodeId(final short[] p_slaveNodeIds) {
        return p_slaveNodeIds[getRandomRangeExclEnd(0, p_slaveNodeIds.length)];
    }

    static short getSuccessorSlaveNodeId(final short[] p_slaveNodeIds, final short p_ownSlaveId) {
        if (p_ownSlaveId + 1 < p_slaveNodeIds.length) {
            return p_slaveNodeIds[p_ownSlaveId + 1];
        } else {
            return p_slaveNodeIds[0];
        }
    }

    static int getRandomSize(final StorageUnit p_start, final StorageUnit p_end) {
        return (int) getRandomRange(p_start.getBytes(), p_end.getBytes());
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
