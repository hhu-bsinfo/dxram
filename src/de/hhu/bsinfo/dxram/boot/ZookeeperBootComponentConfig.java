package de.hhu.bsinfo.dxram.boot;

import java.util.ArrayList;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.unit.IPV4Unit;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Config for the ZookeeperBootComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class ZookeeperBootComponentConfig extends AbstractDXRAMComponentConfig {
    @Expose
    private String m_path = "/dxram";

    @Expose
    private IPV4Unit m_connection = new IPV4Unit("127.0.0.1", 2181);

    @Expose
    private TimeUnit m_timeout = new TimeUnit(10, TimeUnit.SEC);

    @Expose
    private StorageUnit m_bitfieldSize = new StorageUnit(2, StorageUnit.MB);

    @Expose
    private ArrayList<NodesConfiguration.NodeEntry> m_nodesConfig = new ArrayList<NodesConfiguration.NodeEntry>() {
        {
            // default values for local testing
            add(new NodesConfiguration.NodeEntry(new IPV4Unit("127.0.0.1", 22221), (short) 0, (short) 0, NodeRole.SUPERPEER, true));
            add(new NodesConfiguration.NodeEntry(new IPV4Unit("127.0.0.1", 22222), (short) 0, (short) 0, NodeRole.PEER, true));
            add(new NodesConfiguration.NodeEntry(new IPV4Unit("127.0.0.1", 22223), (short) 0, (short) 0, NodeRole.PEER, true));
        }
    };

    /**
     * Constructor
     */
    public ZookeeperBootComponentConfig() {
        super(ZookeeperBootComponent.class, true, true);
    }

    /**
     * Path for zookeeper entry
     */
    public String getPath() {
        return m_path;
    }

    /**
     * Address and port of zookeeper
     */
    public IPV4Unit getConnection() {
        return m_connection;
    }

    /**
     * Zookeeper timeout
     */
    public TimeUnit getTimeout() {
        return m_timeout;
    }

    /**
     * Bloom filter size. Bloom filter is used to increase node ID creation performance.
     */
    public StorageUnit getBitfieldSize() {
        return m_bitfieldSize;
    }

    /**
     * Nodes configuration
     * We can't use the NodesConfiguration class with the configuration because the nodes in that class
     * are already mapped to their node ids
     */
    public ArrayList<NodesConfiguration.NodeEntry> getNodesConfig() {
        return m_nodesConfig;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {

        if (m_bitfieldSize.getBytes() < 2048*1024) {
            // #if LOGGER >= WARN
            LOGGER.warn("Bitfield size is rather small. Not all node IDs may be addressable because of high false positives rate!");
            // #endif /* LOGGER >= WARN */
        }

        return true;
    }
}
