package de.uniduesseldorf.utils.conf;

import org.w3c.dom.Document;

public interface ConfigurationXMLLoader {
	public Document load();
	
	public boolean save(final Document p_document);
}
