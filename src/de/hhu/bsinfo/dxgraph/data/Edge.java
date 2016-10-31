
package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Basic edge object that can be extended with further data if desired.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.09.2016
 */
public class Edge implements DataStructure {

    public static final long INVALID_ID = ChunkID.INVALID_ID;

    private long m_id = ChunkID.INVALID_ID;

    private long m_fromId = ChunkID.INVALID_ID;
    private long m_toId = ChunkID.INVALID_ID;

    /**
     * Constructor
     */
    public Edge() {

    }

    /**
     * Constructor
     * @param p_id
     *            Chunk ID to assign.
     */
    public Edge(final long p_id) {
        m_id = p_id;
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
     * Get the id of the source vertex (directed edge).
     * @return Source vertex id.
     */
    public long getFromId() {
        return m_fromId;
    }

    /**
     * Set the id of the source vertex.
     * @param p_id
     *            Id of the source vertex to set.
     */
    public void setFromId(final long p_id) {
        m_fromId = p_id;
    }

    /**
     * Get the id of the target vertex (directed edge).
     * @return Target vertex id.
     */
    public long getToId() {
        return m_toId;
    }

    /**
     * Set the id of the target vertex.
     * @param p_id
     *            Id of the target vertex to set.
     */
    public void setToId(final long p_id) {
        m_toId = p_id;
    }

    // -----------------------------------------------------------------------------

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_fromId);
        p_exporter.writeLong(m_toId);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_fromId = p_importer.readLong();
        m_toId = p_importer.readLong();
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES * 2;
    }
}
