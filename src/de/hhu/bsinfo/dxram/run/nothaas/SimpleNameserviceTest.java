package de.hhu.bsinfo.dxram.run.nothaas;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Test the nameservice.
 * Run this as a peer and have one superpeer running.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public final class SimpleNameserviceTest extends AbstractMain {

    private static final Argument ARG_CHUNK_SIZE = new Argument("chunkSize", "76", true, "Chunk size for allocation");
    private static final Argument ARG_CHUNK_COUNT = new Argument("chunkCount", "10", true, "Number of chunks to allocate");

    private DXRAM m_dxram;
    private ChunkService m_chunkService;
    private NameserviceService m_nameserviceService;

    /**
     * Constructor
     */
    private SimpleNameserviceTest() {
        super("Simple test case to check if the name service is working");

        m_dxram = new DXRAM();
        m_dxram.initialize("config/dxram.conf");
        m_chunkService = m_dxram.getService(ChunkService.class);
        m_nameserviceService = m_dxram.getService(NameserviceService.class);
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Main arguments.
     */
    public static void main(final String[] p_args) {
        AbstractMain main = new SimpleNameserviceTest();
        main.run(p_args);
    }

    @Override
    protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
        p_arguments.setArgument(ARG_CHUNK_SIZE);
        p_arguments.setArgument(ARG_CHUNK_COUNT);
    }

    @Override
    protected int main(final ArgumentList p_arguments) {
        final int size = p_arguments.getArgument(ARG_CHUNK_SIZE).getValue(Integer.class);
        final int chunkCount = p_arguments.getArgument(ARG_CHUNK_COUNT).getValue(Integer.class);

        Chunk[] chunks = new Chunk[chunkCount];
        int[] sizes = new int[chunkCount];
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = size;
        }

        System.out.println("Creating chunks...");
        long[] chunkIDs = m_chunkService.createSizes(sizes);

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new Chunk(chunkIDs[i], sizes[i]);
        }

        Map<Long, String> verificationMap = new HashMap<Long, String>();

        System.out.println("Registering " + chunkCount + " chunks...");
        for (int i = 0; i < chunks.length; i++) {
            String name = "I-" + i;
            m_nameserviceService.register(chunks[i], name);
            verificationMap.put(chunks[i].getID(), name);
        }

        System.out.println("Getting chunk IDs from name service and verifying...");
        for (Entry<Long, String> entry : verificationMap.entrySet()) {
            long chunkID = m_nameserviceService.getChunkID(entry.getValue(), -1);
            if (chunkID != entry.getKey()) {
                System.out.println(
                    "ChunkID from name service (" + ChunkID.toHexString(entry.getKey()) + ") not matching original chunk ID (" + ChunkID.toHexString(chunkID) +
                        "), for name " + entry.getValue() + '.');
            }
        }

        System.out.println("Done.");
        return 0;
    }
}
