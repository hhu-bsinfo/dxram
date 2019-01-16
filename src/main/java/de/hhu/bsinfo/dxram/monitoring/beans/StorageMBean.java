package de.hhu.bsinfo.dxram.monitoring.beans;

public interface StorageMBean {

    long getAllocatedChunks();

    long getFreeMemory();

    double getFragmentation();

    long allocateChunk(int p_size);
}
