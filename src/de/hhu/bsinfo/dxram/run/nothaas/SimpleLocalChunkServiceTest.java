package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Minimal (local only) ChunkService test.
 * Run this as a peer with one superpeer.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public final class SimpleLocalChunkServiceTest extends AbstractMain {
    private DXRAM m_dxram;
    private ChunkService m_chunkService;

    /**
     * Constructor
     */
    private SimpleLocalChunkServiceTest() {
        super("Simple test to verify if the local chunk service is working");
        m_dxram = new DXRAM();
        m_dxram.initialize(true);
        m_chunkService = m_dxram.getService(ChunkService.class);
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Main arguments.
     */
    public static void main(final String[] p_args) {
        AbstractMain main = new SimpleLocalChunkServiceTest();
        main.run(p_args);
    }

    @Override
    protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
    }

    @Override
    protected int main(final ArgumentList p_arguments) {
        int[] sizes = new int[] {100, 32, 432, 8};
        System.out.println("Creating chunks...");
        long[] chunkIDs = m_chunkService.createSizes(sizes);
        Chunk[] chunks = new Chunk[chunkIDs.length];
        Chunk[] chunksCopy = new Chunk[chunkIDs.length];
        for (int i = 0; i < chunkIDs.length; i++) {
            chunks[i] = new Chunk(chunkIDs[i], sizes[i]);
            chunksCopy[i] = new Chunk(chunkIDs[i], sizes[i]);
        }

        System.out.println("Chunks created: ");
        for (int i = 0; i < chunkIDs.length; i++) {
            System.out.println(chunks[i]);
        }

        System.out.println("Setting chunk payload...");
        for (Chunk chunk : chunks) {
            System.out.println(ChunkID.toHexString(chunk.getID()) + ": " + ChunkID.toHexString(chunk.getID()));
            chunk.getData().putLong(chunk.getID());
        }

        System.out.println("Putting chunks...");
        int ret = m_chunkService.put(chunks);
        System.out.println("Putting chunks results: " + ret);

        System.out.println("Getting chunks...");
        ret = m_chunkService.get(chunksCopy);
        System.out.println("Getting chunks restults: " + ret);

        System.out.println("Data got: ");
        for (Chunk chunk : chunksCopy) {
            System.out.println(ChunkID.toHexString(chunk.getData().getLong()));
        }

        System.out.println("Removing chunks...");
        int removeCount = m_chunkService.remove(chunks);
        System.out.println("Removed chunks: " + removeCount);

        return 0;
    }
}
