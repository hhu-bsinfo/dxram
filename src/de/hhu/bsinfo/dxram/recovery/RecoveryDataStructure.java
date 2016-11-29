package de.hhu.bsinfo.dxram.recovery;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Object to hand over recovered data to memory management.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 08.11.2016
 */
public class RecoveryDataStructure implements DataStructure {
    private long m_id = ChunkID.INVALID_ID;

    private byte[] m_data;
    private int m_offset;
    private int m_length;

    /**
     * Constructor
     */
    public RecoveryDataStructure() {

    }

    /**
     * Constructor
     *
     * @param p_id
     *     Chunk ID to assign.
     */
    public RecoveryDataStructure(final byte[] p_data) {
        m_data = p_data;
        m_offset = 0;
        m_length = 0;
    }

    // -----------------------------------------------------------------------------

    @Override
    public long getID() {
        return m_id;
    }

    @Override
    public void setID(long p_id) {
        m_id = p_id;
    }

    /**
     * Set the id of the target vertex.
     *
     * @param p_id
     *     Id of the target vertex to set.
     */
    public int getLength() {
        return m_length;
    }

    /**
     * Set the id of the target vertex.
     *
     * @param p_id
     *     Id of the target vertex to set.
     */
    public void setLength(final int p_length) {
        m_length = p_length;
    }

    /**
     * Set the id of the source vertex.
     *
     * @param p_id
     *     Id of the source vertex to set.
     */
    public void setOffset(final int p_offset) {
        m_offset = p_offset;
    }

    // -----------------------------------------------------------------------------

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(m_data, m_offset, m_length);
    }

    @Override
    public void importObject(final Importer p_importer) {
        // unused
    }

    @Override
    public int sizeofObject() {
        return m_length;
    }
}
