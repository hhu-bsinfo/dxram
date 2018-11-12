package de.hhu.bsinfo.dxram.boot;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

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
     * Path for zookeeper entry
     */
    @Expose
    private String m_path = "/dxram";

    /**
     * Address and port of zookeeper
     */
    @Expose
    private IPV4Unit m_connection = new IPV4Unit("127.0.0.1", 2181);

    @Expose
    private TimeUnit m_timeout = new TimeUnit(10, TimeUnit.SEC);

    /**
     * Bloom filter size. Bloom filter is used to increase node ID creation performance.
     */
    @Expose
    private StorageUnit m_bitfieldSize = new StorageUnit(2, StorageUnit.MB);

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

    @Expose
    private boolean m_isClient = false;

    /**
     * Constructor
     */
    public ZookeeperBootComponentConfig() {
        super(ZookeeperBootComponent.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        if (m_bitfieldSize.getBytes() < 2048 * 1024) {
            LOGGER.warn("Bitfield size is rather small. Not all node IDs may be addressable because of high " +
                    "false positives rate!");
        }

        if (p_config.getEngineConfig().getRole() == NodeRole.SUPERPEER && m_isClient) {
            LOGGER.error("Client nodes can't be superpeers");
            return false;
        }

        return true;
    }
}
