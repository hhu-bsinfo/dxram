package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.util.NodeRole;

public interface TerminalDelegate {
	public void exitTerminal();
	
	public NodeRole nodeExists(final short nodeID);
	
	public <T extends DXRAMService> T getService(final Class<T> p_class); 
}
