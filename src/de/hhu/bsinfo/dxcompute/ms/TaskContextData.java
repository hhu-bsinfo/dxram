
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Data for a task that describes the context.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.10.2016
 */
public class TaskContextData implements Importable, Exportable {

    private short m_computeGroupId = -1;
    private short m_slaveId = -1;
    // list of all slaves of the same compute group sorted by their slave id (indexable)
    private short[] m_slaveNodeIds = new short[0];

    /**
     * Default constructor (for importing)
     */
    public TaskContextData() {

    }

    /**
     * Constructor
     * @param p_computeGroupId
     *            Compute group id the task is assigned to.
     * @param p_slaveId
     *            Id of the slave the task is assigned to.
     * @param p_slaveNodeIds
     *            List of node ids of all involved slaves with this task
     */
    public TaskContextData(final short p_computeGroupId, final short p_slaveId, final short[] p_slaveNodeIds) {
        m_computeGroupId = p_computeGroupId;
        m_slaveId = p_slaveId;
        m_slaveNodeIds = p_slaveNodeIds;
    }

    /**
     * Get the compute group id this task is executed in.
     * @return Compute group id.
     */
    public short getComputeGroupId() {
        return m_computeGroupId;
    }

    /**
     * Get the id of the slave that executes the task (0 based).
     * @return Id of the slave executing the task.
     */
    public short getSlaveId() {
        return m_slaveId;
    }

    /**
     * Get the node ids of all slaves executing this task.
     * @return Node ids of all slaves. Indexable by slave id.
     */
    public short[] getSlaveNodeIds() {
        return m_slaveNodeIds;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeShort(m_computeGroupId);
        p_exporter.writeShort(m_slaveId);
        p_exporter.writeInt(m_slaveNodeIds.length);
        p_exporter.writeShorts(m_slaveNodeIds);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_computeGroupId = p_importer.readShort();
        m_slaveId = p_importer.readShort();
        m_slaveNodeIds = new short[p_importer.readInt()];
        p_importer.readShorts(m_slaveNodeIds);
    }

    @Override
    public int sizeofObject() {
        return 2 * Short.BYTES + Integer.BYTES + m_slaveNodeIds.length * Short.BYTES;
    }

    @Override
    public String toString() {
        return "[" + m_computeGroupId + ", " + m_slaveId + "/" + m_slaveNodeIds.length + "]";
    }
}
