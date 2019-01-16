package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Minimal job to test service access inside a Job
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class JobAccessTest extends AbstractJob {
    public static final short MS_TYPE_ID = 2;

    private short m_nodeId;

    /**
     * Constructor
     */
    public JobAccessTest() {
        m_nodeId = NodeID.INVALID_ID;
    }

    /**
     * Get the node ID result
     *
     * @return Node ID
     */
    public int getNodeID() {
        return m_nodeId;
    }

    @Override
    public short getTypeID() {
        return MS_TYPE_ID;
    }

    @Override
    public void execute() {
        m_nodeId = getService(BootService.class).getNodeID();
    }
}
