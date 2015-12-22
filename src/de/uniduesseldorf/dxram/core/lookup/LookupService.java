package de.uniduesseldorf.dxram.core.lookup;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.engine.DXRAMService;
import de.uniduesseldorf.utils.config.Configuration;

// TODO move many things from the LookupComponent to this class:
// - network message handling
// - connection lost handling
public class LookupService extends DXRAMService {

	public static final String SERVICE_NAME = "Lookup";
	
	private final Logger LOGGER = Logger.getLogger(LookupService.class);
	
	public LookupService() {
		super(SERVICE_NAME);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected boolean startService(Configuration p_configuration) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean shutdownService() {
		// TODO Auto-generated method stub
		return false;
	}

	
}
