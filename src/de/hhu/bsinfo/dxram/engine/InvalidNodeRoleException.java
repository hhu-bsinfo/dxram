package de.hhu.bsinfo.dxram.engine;

import de.hhu.bsinfo.dxram.util.NodeRole;

public class InvalidNodeRoleException extends DXRAMRuntimeException {
    public InvalidNodeRoleException(final NodeRole p_role) {
        super("Invalid node role " + p_role);
    }
}
