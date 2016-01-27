package de.hhu.bsinfo.dxram.lookup.event;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.event.Event;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.dxram.util.NodeRole;

public class NodeFailureEvent extends Event {

	private short m_nodeID = NodeID.INVALID_ID;
	private NodeRole m_role = NodeRole.PEER;
	
	public NodeFailureEvent(final Class<? extends DXRAMComponent> p_sourceClass, final short p_nodeID, final NodeRole p_role) {
		super(p_sourceClass);
		
		m_nodeID = p_nodeID;
		m_role = p_role;
	}

	public short getNodeID()
	{
		return m_nodeID;
	}
	
	public NodeRole getRole()
	{
		return m_role;
	}
}
