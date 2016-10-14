package de.hhu.bsinfo.dxram.script;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;

/**
 * Created by nothaas on 10/14/16.
 */
public interface ScriptDXRAMContext {

	void list();

	AbstractDXRAMService service(final String p_serviceName);

	String nodeIdToHexString(final short nodeId);
}
