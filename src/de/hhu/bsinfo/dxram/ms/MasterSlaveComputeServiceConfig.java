package de.hhu.bsinfo.dxram.ms;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Config for the MasterSlaveComputeService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class MasterSlaveComputeServiceConfig extends DXRAMServiceConfig {
    @Expose
    private String m_role = ComputeRole.NONE.toString();

    @Expose
    private short m_computeGroupId = 0;

    @Expose
    private TimeUnit m_pingInterval = new TimeUnit(1, TimeUnit.SEC);

    /**
     * Constructor
     */
    public MasterSlaveComputeServiceConfig() {
        super(MasterSlaveComputeService.class, false, true);
    }

    /**
     * Compute role to assign to the current instance (master, slave or none)
     */
    public String getRole() {
        return m_role;
    }

    /**
     * Compute group id for the current instance (ignored on none)
     */
    public short getComputeGroupId() {
        return m_computeGroupId;
    }

    /**
     * Keep alive ping time for master to contact slaves
     */
    public TimeUnit getPingInterval() {
        return m_pingInterval;
    }
}
