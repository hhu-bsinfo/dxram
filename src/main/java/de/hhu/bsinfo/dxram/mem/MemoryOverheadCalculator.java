package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxutils.unit.StorageUnit;

public class MemoryOverheadCalculator
{
    private static final int SIZE_MARKER_BYTE = 1;
    private static final int NID_TABLE_ENTRY_COUNT = (int) Math.pow(2, 16);
    private static final int LID_TABLE_ENTRY_COUNT = (int) Math.pow(2, 12);
    private static final int LID_TABLE_LEVEL_COUNT = 4;

    public static void main(final String[] p_args) {
        if (p_args.length < 3) {
            System.out.println("Calculate the required space and overhead of the memory manager");
            System.out.println("Usage: <ptrSize> <chunkPayloadSize> <totalChunkCount>");
        }

        int ptrSize = Integer.parseInt(p_args[0]);
        int chunkPayloadSize = Integer.parseInt(p_args[1]);
        long totalChunkCount = Long.parseLong(p_args[2]);

        StorageUnit totalPayloadMem = new StorageUnit(totalChunkCount * chunkPayloadSize, "b");
        StorageUnit chunkSizeMemory = new StorageUnit(calcTotalChunkSizeMemory(chunkPayloadSize, totalChunkCount), "b");
        StorageUnit nidTableSize = new StorageUnit(calcSizeNIDTable(ptrSize), "b");
        StorageUnit[] lidTableSizes = new StorageUnit[LID_TABLE_LEVEL_COUNT];

        for (int i = 0; i < lidTableSizes.length; i++) {
            lidTableSizes[i] = new StorageUnit(calcSizeLIDTable(ptrSize, i, totalChunkCount), "b");
        }

        long tmp = 0;

        tmp += chunkSizeMemory.getBytes();
        tmp += nidTableSize.getBytes();

        for (StorageUnit lidTableSize : lidTableSizes) {
            tmp += lidTableSize.getBytes();
        }

        StorageUnit totalMem = new StorageUnit(tmp, "b");
        StorageUnit overheadMem = new StorageUnit(totalMem.getBytes() - totalPayloadMem.getBytes(), "b");
        double overhead = ((double) overheadMem.getBytes() / totalMem.getBytes()) * 100.0;

        System.out.print("Parameters:\n");
        System.out.printf("Ptr size: %d\n", ptrSize);
        System.out.printf("Chunk payload size: %s\n", new StorageUnit(chunkPayloadSize, "b"));
        System.out.printf("Total chunk count: %d\n", totalChunkCount);
        System.out.print("-------------------\n");
        System.out.printf("Total chunks size: %s\n", chunkSizeMemory);
        System.out.printf("Nid table size: %s\n", nidTableSize);

        for (int i = 0; i < lidTableSizes.length; i++) {
            System.out.printf("Lid table level %d size: %s\n", i, lidTableSizes[i]);
        }

        System.out.print("-------------------\n");
        System.out.printf("Total payload memory: %s\n", totalPayloadMem);
        System.out.printf("Total overhead memory: %s\n", overheadMem);
        System.out.printf("Total memory with overhead: %s\n", totalMem);
        System.out.printf("Overhead: %f\n", overhead);
    }

    private static long calcSizeLIDTable(final int p_ptrSize, final int p_tableLevel, final long p_totalNumChunks) {
        // round up to full lid tables
        return (long) Math.ceil(p_totalNumChunks / Math.pow(2, 12 * (p_tableLevel + 1))) * LID_TABLE_ENTRY_COUNT * p_ptrSize;
    }

    private static long calcSizeNIDTable(final int p_ptrSize) {
        return (long) NID_TABLE_ENTRY_COUNT * p_ptrSize;
    }

    private static long calcTotalChunkSizeMemory(final int p_chunkPayloadSize, final long p_totalChunkCount) {
        return calcTotalChunkSizeMemory(p_chunkPayloadSize) * p_totalChunkCount;
    }

    private static long calcTotalChunkSizeMemory(final int p_chunkPayloadSize) {
        return SIZE_MARKER_BYTE + calcSizeLengthFieldChunk(p_chunkPayloadSize) + p_chunkPayloadSize;
    }

    private static int calcSizeLengthFieldChunk(final int p_chunkPayloadSize) {
        if (p_chunkPayloadSize < Math.pow(2, 8)) {
            return 1;
        } else if (p_chunkPayloadSize < Math.pow(2, 16)) {
            return 2;
        } else if (p_chunkPayloadSize < Math.pow(2, 24)) {
            return 3;
        } else {
            return 4;
        }
    }
}
