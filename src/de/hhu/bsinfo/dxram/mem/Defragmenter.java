package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.soh.SmallObjectHeap;

// TODO draft of simple defragmenter (untested
public final class Defragmenter {

    private static final long SLEEP_TIME = 10000;
    private static final double MAX_FRAGMENTATION = 0.75;

    private CIDTable m_cidTable;
    private SmallObjectHeap m_heap;

    private long[] m_curTables = new long[CIDTable.LID_TABLE_LEVELS + 1];
    private int[] m_curTableIndex = new int[CIDTable.LID_TABLE_LEVELS + 1];

    private boolean m_initDefragPos;

    /**
     * Creates an instance of Defragmenter_old
     */
    public Defragmenter(final CIDTable p_cidTable, final SmallObjectHeap p_heap) {
        m_cidTable = p_cidTable;
        m_heap = p_heap;

        for (int i = 0; i < CIDTable.LID_TABLE_LEVELS + 1; i++) {
            m_curTables[i] = -1;
            m_curTableIndex[i] = -1;
        }

        m_initDefragPos = false;
    }

    public void initDefragmentPosition() {

        m_curTables[0] = m_cidTable.getAddressTableDirectory();

        for (int i = 0; i < m_curTables.length; i++) {
            m_curTableIndex[i] = 0;
        }

        for (int i = 0; i < m_curTables.length - 1; i++) {
            m_curTables[i + 1] = m_cidTable.readEntry(m_curTables[i], m_curTableIndex[i]);
        }
    }

    public long getAddressCurrentEntry() {
        return m_cidTable.readEntry(m_curTables[m_curTables.length - 1], m_curTableIndex[m_curTables.length - 1]);
    }

    public void replaceAddressCurrenEntry(final long p_newAddress) {
        m_cidTable.writeEntry(m_curTables[m_curTables.length - 1], m_curTableIndex[m_curTables.length - 1], p_newAddress);
    }

    public void moveNextEntry() {

        for (int i = m_curTables.length - 1; i >= 0; i--) {
            m_curTableIndex[i]++;

            if (i == 0) {
                if (m_curTableIndex[i] > CIDTable.ENTRIES_FOR_NID_LEVEL) {
                    m_curTableIndex[i] = 0;
                }
            } else {
                if (m_curTableIndex[i] > CIDTable.ENTRIES_PER_LID_LEVEL) {
                    m_curTableIndex[i] = 0;
                } else {
                    break;
                }
            }
        }
    }

    public void defragment(final int p_entries) {

        long address;
        long newAddress;
        byte[] data;

        if (!m_initDefragPos) {
            initDefragmentPosition();
            m_initDefragPos = true;
        }

        for (int i = 0; i < p_entries; i++) {

            while (true) {
                address = getAddressCurrentEntry();
                if (address != 0) {
                    break;
                }
                moveNextEntry();
            }

            data = new byte[m_heap.getSizeBlock(address)];

            if (m_heap.readBytes(address, 0, data, 0, data.length) != data.length) {
                // TODO error handling
            }

            newAddress = m_heap.malloc(data.length);
            if (m_heap.writeBytes(newAddress, 0, data, 0, data.length) != data.length) {
                // TODO error handling
            }

            replaceAddressCurrenEntry(newAddress);

            m_heap.free(address);

            moveNextEntry();
        }
    }
}
