package de.hhu.bsinfo.dxram.chunk.data;

import de.hhu.bsinfo.dxmem.core.CIDTableStatus;
import de.hhu.bsinfo.dxmem.core.HeapStatus;
import de.hhu.bsinfo.dxmem.core.LIDStoreStatus;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Wrapper object for various status objects of the memory management
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class ChunkServiceStatus implements Importable, Exportable {
    private HeapStatus m_heapStatus;
    private CIDTableStatus m_cidTableStatus;
    private LIDStoreStatus m_lidStoreStatus;

    /**
     * Constructor
     */
    public ChunkServiceStatus() {
        m_heapStatus = new HeapStatus();
        m_cidTableStatus = new CIDTableStatus();
        m_lidStoreStatus = new LIDStoreStatus();
    }

    /**
     * Constructor
     *
     * @param p_heapStatus
     *         HeapStatus object
     * @param p_cidTableStatus
     *         CIDTableStatus object
     * @param p_lidStoreStatus
     *         LIDStoreStatus object
     */
    public ChunkServiceStatus(final HeapStatus p_heapStatus, final CIDTableStatus p_cidTableStatus,
            final LIDStoreStatus p_lidStoreStatus) {
        m_heapStatus = p_heapStatus;
        m_cidTableStatus = p_cidTableStatus;
        m_lidStoreStatus = p_lidStoreStatus;
    }

    /**
     * Get the heap status
     *
     * @return Heap status
     */
    public HeapStatus getHeapStatus() {
        return m_heapStatus;
    }

    /**
     * Get the CID table status
     *
     * @return CID table status
     */
    public CIDTableStatus getCIDTableStatus() {
        return m_cidTableStatus;
    }

    /**
     * Get the LID store status
     *
     * @return LID store status
     */
    public LIDStoreStatus getLIDStoreStatus() {
        return m_lidStoreStatus;
    }

    @Override
    public String toString() {
        return "HeapStatus: " + m_heapStatus + "\nCIDTableStatus: " + m_cidTableStatus + "\nLIDStoreStatus: " +
                m_lidStoreStatus;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.exportObject(m_heapStatus);
        p_exporter.exportObject(m_cidTableStatus);
        p_exporter.exportObject(m_lidStoreStatus);
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.importObject(m_heapStatus);
        p_importer.importObject(m_cidTableStatus);
        p_importer.importObject(m_lidStoreStatus);
    }

    @Override
    public int sizeofObject() {
        return m_heapStatus.sizeofObject() + m_cidTableStatus.sizeofObject() + m_lidStoreStatus.sizeofObject();
    }
}
