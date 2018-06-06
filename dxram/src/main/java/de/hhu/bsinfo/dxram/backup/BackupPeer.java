package de.hhu.bsinfo.dxram.backup;

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class BackupPeer implements Importable, Exportable {
    private short m_nodeID;
    private short m_rack;
    private short m_switch;

    /**
     * Creates an instance of BackupPeer
     *
     * @param p_nodeID
     *         the NodeID
     * @param p_rack
     *         the rack
     * @param p_switch
     *         the switch
     */
    public BackupPeer(final short p_nodeID, final short p_rack, final short p_switch) {
        m_nodeID = p_nodeID;
        m_rack = p_rack;
        m_switch = p_switch;
    }

    /**
     * Returns the NodeID
     *
     * @return the NodeID
     */
    public short getNodeID() {
        return m_nodeID;
    }

    /**
     * Returns the rack
     *
     * @return the rack
     */
    public short getRack() {
        return m_rack;
    }

    /**
     * Returns the switch
     *
     * @return the switch
     */
    public short getSwitch() {
        return m_switch;
    }

    @Override
    public String toString() {
        return "[" + m_nodeID + ", " + m_rack + ", " + m_switch + ']';
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeShort(m_nodeID);
        p_exporter.writeShort(m_rack);
        p_exporter.writeShort(m_switch);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_nodeID = p_importer.readShort(m_nodeID);
        m_rack = p_importer.readShort(m_rack);
        m_switch = p_importer.readShort(m_switch);
    }

    @Override
    public int sizeofObject() {
        return 3 * Short.BYTES;
    }
}
