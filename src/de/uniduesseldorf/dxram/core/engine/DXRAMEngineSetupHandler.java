package de.uniduesseldorf.dxram.core.engine;

import de.uniduesseldorf.utils.config.Configuration;

public interface DXRAMEngineSetupHandler 
{
	void setupComponents(final DXRAMEngine p_engine, final Configuration p_configuration);
	
	void setupServices(final DXRAMEngine p_engine, final Configuration p_configuration);
}
