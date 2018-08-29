package de.hhu.bsinfo.dxram.boot;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.ArrayList;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

import static de.hhu.bsinfo.dxram.util.NodeCapabilities.COMPUTE;
import static de.hhu.bsinfo.dxram.util.NodeCapabilities.NONE;
import static de.hhu.bsinfo.dxram.util.NodeCapabilities.STORAGE;
import static de.hhu.bsinfo.dxram.util.NodeCapabilities.toMask;

/**
 * Config for the ZookeeperBootComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = ZookeeperBootComponent.class, supportsSuperpeer = true, supportsPeer = true)
public class ZookeeperBootComponentConfig extends DXRAMComponentConfig {
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
            add(new NodesConfiguration.NodeEntry(new IPV4Unit("127.0.0.1", 22221), NodeID.INVALID_ID, (short) 0,
                    (short) 0, NodeRole.SUPERPEER, NONE, true, true, false));
            add(new NodesConfiguration.NodeEntry(new IPV4Unit("127.0.0.1", 22222), NodeID.INVALID_ID, (short) 0,
                    (short) 0, NodeRole.PEER, toMask(STORAGE, COMPUTE), true, true, false));
            add(new NodesConfiguration.NodeEntry(new IPV4Unit("127.0.0.1", 22223), NodeID.INVALID_ID, (short) 0,
                    (short) 0, NodeRole.PEER, toMask(STORAGE, COMPUTE), true, true, false));
        }
    };

    @Expose
    private short m_rack = 0;

    @Expose
    private short m_switch = 0;

    @Expose
    private boolean m_isClient = false;

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
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
