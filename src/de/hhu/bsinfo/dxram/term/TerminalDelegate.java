package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMService;

// we allow the commands to have some special "rights"
// and grant access to both components and services to allow
// easier development of new commands for gathering information etc
public interface TerminalDelegate {
	public boolean areYouSure();
	
	public void exitTerminal();
	
	public <T extends DXRAMService> T getDXRAMService(final Class<T> p_class);
	
	public <T extends DXRAMComponent> T getDXRAMComponent(final Class<T> p_class);
}
