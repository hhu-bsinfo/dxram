package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.eval.Stopwatch;
import de.hhu.bsinfo.utils.main.AbstractMain;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Very simple/minimal benchmark to test and verify the core operations
 * of the ChunkService.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public final class LinkedListBenchmark extends AbstractMain {
    private static final Argument ARG_ITEM_COUNT = new Argument("itemCount", "100", true, "Number of items for the linked list");

    private DXRAM m_dxram;
    private ChunkService m_chunkService;
    private StatisticsService m_statisticsService;
    private Stopwatch m_stopwatch = new Stopwatch();

    /**
     * Constructor
     */
    private LinkedListBenchmark() {
        super("Small benchmark, which creates a linked list in DXRAM and iterates it");

        m_dxram = new DXRAM();
        m_dxram.initialize("config/dxram.conf");
        m_chunkService = m_dxram.getService(ChunkService.class);
        m_statisticsService = m_dxram.getService(StatisticsService.class);
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Main arguments.
     */
    public static void main(final String[] p_args) {
        AbstractMain main = new LinkedListBenchmark();
        main.run(p_args);
    }

    @Override
    protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
        p_arguments.setArgument(ARG_ITEM_COUNT);
    }

    @Override
    protected int main(final ArgumentList p_arguments) {
        final int itemCount = p_arguments.getArgument(ARG_ITEM_COUNT).getValue(Integer.class);

        System.out.println("Creating linked list with " + itemCount + " items.");
        m_stopwatch.start();
        long listHead = createLinkedList(itemCount);
        m_stopwatch.stop();
        m_stopwatch.print("create", true);
        System.out.println("Done creating linked list.");

        System.out.println("Walking linked list, head " + listHead);
        m_stopwatch.start();
        long itemsTouched = walkLinkedList(listHead);
        m_stopwatch.stop();
        m_stopwatch.print("walk", true);
        System.out.println("Walking linked list done, total elements touched: " + itemsTouched);

        System.out.println("Done");

        StatisticsService.printStatistics();

        return 0;
    }

    /**
     * Create the linked list using the ChunkService.
     *
     * @param p_numItems
     *     Length of the linked list.
     * @return ChunkID of the root.
     */
    private long createLinkedList(final int p_numItems) {
        LinkedListElement[] chunks = new LinkedListElement[p_numItems];
        long[] chunkIDs = m_chunkService.create(8, p_numItems);
        LinkedListElement head = null;
        LinkedListElement previousChunk = null;

        for (int i = 0; i < chunkIDs.length; i++) {
            chunks[i] = new LinkedListElement(chunkIDs[i]);
            if (previousChunk == null) {
                // init head
                head = chunks[i];
                previousChunk = head;
            } else {
                previousChunk.setNextID(chunks[i].getID());
                previousChunk = chunks[i];
            }
        }

        // mark end
        chunks[chunks.length - 1].setNextID(-1);

        if (m_chunkService.put(chunks) != chunks.length) {
            System.out.println("Putting linked list failed.");
            return -1;
        }

        assert head != null;
        return head.getID();
    }

    /**
     * Walk the linked list.
     *
     * @param p_headChunkID
     *     Head of the linked list to start at.
     * @return Number of visited elements.
     */
    private long walkLinkedList(final long p_headChunkID) {
        long counter = 0;
        LinkedListElement chunk = new LinkedListElement(p_headChunkID);
        if (m_chunkService.get(chunk) != 1) {
            System.out.println("Getting head chunk if linked list failed.");
            return 0;
        }
        counter++;

        while (true) {
            long nextChunkID = chunk.getNextID();
            if (nextChunkID == -1) {
                break;
            }
            // reuse same chunk to avoid allocations
            chunk.setOwnID(nextChunkID);
            m_chunkService.get(chunk);
            counter++;
        }

        return counter;
    }

    /**
     * Simple linked list element.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
     */
    private static class LinkedListElement implements DataStructure {
        private long m_ownID = -1;
        private long m_nextID = -1;

        /**
         * Constructor
         *
         * @param p_ownID
         *     ChunkID of the element.
         */
        LinkedListElement(final long p_ownID) {
            m_ownID = p_ownID;
        }

        /**
         * Get the chunkID of the next element.
         *
         * @return ChunkID.
         */
        long getNextID() {
            return m_nextID;
        }

        /**
         * Set the chunkID of the next element.
         *
         * @param p_nextID
         *     ChunkID.
         */
        void setNextID(final long p_nextID) {
            m_nextID = p_nextID;
        }

        @Override
        public long getID() {
            return m_ownID;
        }

        @Override
        public void setID(final long p_id) {
            m_ownID = p_id;
        }

        /**
         * Set the chunkID of the element.
         *
         * @param p_ownID
         *     ChunkID.
         */
        void setOwnID(final long p_ownID) {
            m_ownID = p_ownID;
        }

        @Override
        public void importObject(final Importer p_importer) {
            m_nextID = p_importer.readLong();
        }

        @Override
        public int sizeofObject() {
            return Long.BYTES;
        }

        @Override
        public void exportObject(final Exporter p_exporter) {
            p_exporter.writeLong(m_nextID);
        }

    }
}
