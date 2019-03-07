package de.hhu.bsinfo.dxram.boot;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Config for the ZookeeperBootComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class ZookeeperBootComponentConfig extends DXRAMModuleConfig {

    /**
     * Path for zookeeper entry.
     */
    @Expose
    private String m_path = "/dxram";

    /**
     * Path for zookeeper data directory.
     */
    @Expose
    private String m_dataDir = DXRAM.getAbsolutePath("zookeeper_data");

    /**
     * Address and port of zookeeper (bootstrap peer).
     */
    @Expose
    private IPV4Unit m_connection = new IPV4Unit("127.0.0.1", 2181);

    /**
     * The ZooKeeper connection timeout.
     */
    @Expose
    private TimeUnit m_timeout = new TimeUnit(10, TimeUnit.SEC);

    /**
     * The rack this node is in. Must be set if node was not in initial nodes file.
     */
    @Expose
    private short m_rack = 0;

    /**
     * The switch this node is connected to. Must be set if node was not in initial nodes file.
     */
    @Expose
    private short m_switch = 0;

    /**
     * Indicates if this peer is responsible for the bootstrapping process.
     */
    @Expose
    private boolean m_isBootstrap = false;

    /**
     * Constructor
     */
    public ZookeeperBootComponentConfig() {
        super(ZookeeperBootComponent.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        if (p_config.getEngineConfig().getRole() != NodeRole.SUPERPEER && m_isBootstrap) {
            LOGGER.error("Bootstrap nodes must be superpeers");
            return false;
        }

        return true;
    }
}
