package de.hhu.bsinfo.dxram.ms;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class TestIncChunkTask implements Task {
    private long m_chunkId;

    public TestIncChunkTask() {

    }

    public TestIncChunkTask(final long p_chunkId) {
        m_chunkId = p_chunkId;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);

        System.out.println("Task on slave " + p_ctx.getCtxData().getSlaveId() + ", chunk ID " +
                ChunkID.toHexString(m_chunkId));

        // avoid fighting over lock which is very slow when testing on localhost
        try {
            Thread.sleep(500 * p_ctx.getCtxData().getSlaveId());
        } catch (final InterruptedException ignore) {

        }

        TestIncChunk chunk = new TestIncChunk();
        chunk.setID(m_chunkId);

        // use high try lock timeouts for localhost tests
        if (!chunkService.get().get(chunk, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, 10000)) {
            throw new RuntimeException("Getting chunk failed");
        }

        chunk.incCounter();

        // use high try lock timeouts for localhost tests
        if (!chunkService.put().put(chunk, ChunkLockOperation.WRITE_LOCK_REL_POST_OP, 10000)) {
            throw new RuntimeException("Putting chunk failed");
        }

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {

    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_chunkId);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_chunkId = p_importer.readLong(m_chunkId);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES;
    }
}
