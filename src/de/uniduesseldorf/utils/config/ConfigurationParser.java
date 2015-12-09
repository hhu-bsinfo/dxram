package de.uniduesseldorf.utils.config;

public interface ConfigurationParser {
	void readConfiguration(final Configuration p_configuration) throws ConfigurationException;
	
	void writeConfiguration(final Configuration p_configuration) throws ConfigurationException;
}
