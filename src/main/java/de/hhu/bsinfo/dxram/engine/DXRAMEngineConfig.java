package de.hhu.bsinfo.dxram.engine;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Configuration for the engine
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.08.2017
 */
@Data
@Accessors(prefix = "m_")
@ToString
public class DXRAMEngineConfig {
    /**
     * Address and port of this instance
     */
    @Expose
    private IPV4Unit m_address = new IPV4Unit("127.0.0.1", 22222);

    /**
     * Role of this instance (superpeer, peer, terminal)
     */
    @Expose
    private String m_role = "Peer";

    /**
     * Path to jni dependencies
     */
    @Expose
    private String m_jniPath = DXRAM.getAbsolutePath("jni");

    /**
     * Role assigned for this DXRAM instance
     *
     * @return Role
     */
    public NodeRole getRole() {
        return NodeRole.toNodeRole(m_role);
    }
}
