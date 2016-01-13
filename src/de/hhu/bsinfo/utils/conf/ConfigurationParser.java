package de.hhu.bsinfo.utils.conf;

import de.hhu.bsinfo.utils.conf.Configuration;
import de.hhu.bsinfo.utils.conf.ConfigurationException;

public interface ConfigurationParser 
{
	void readConfiguration(final Configuration p_configuration) throws ConfigurationException;
	
	void writeConfiguration(final Configuration p_configuration) throws ConfigurationException;
}
