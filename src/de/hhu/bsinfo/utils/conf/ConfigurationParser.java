
package de.hhu.bsinfo.utils.conf;


public interface ConfigurationParser
{
	void readConfiguration(final Configuration p_configuration) throws ConfigurationException;

	void writeConfiguration(final Configuration p_configuration) throws ConfigurationException;
}
