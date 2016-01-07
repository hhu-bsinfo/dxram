package de.uniduesseldorf.utils.conf;

import de.uniduesseldorf.utils.conf.Configuration;
import de.uniduesseldorf.utils.conf.ConfigurationException;

public interface ConfigurationParser 
{
	void readConfiguration(final Configuration p_configuration) throws ConfigurationException;
	
	void writeConfiguration(final Configuration p_configuration) throws ConfigurationException;
}
