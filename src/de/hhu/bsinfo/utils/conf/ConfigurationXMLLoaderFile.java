package de.hhu.bsinfo.utils.conf;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class ConfigurationXMLLoaderFile implements ConfigurationXMLLoader
{
	private String m_path = null;
	
	public ConfigurationXMLLoaderFile(final String p_path)
	{
		m_path = p_path;
	}
	
	@Override
	public Document load() {
		Document document = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		try {
			builder = factory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			return null;
		}
		
		try {
			document = builder.parse(new File(m_path));
		} catch (SAXException | IOException e) {
			return null;
		}
		
		return document;
	}

	@Override
	public boolean save(Document p_document) 
	{
		 TransformerFactory transformerFactory = TransformerFactory.newInstance();
         Transformer transformer;
		try {
			transformer = transformerFactory.newTransformer();
		} catch (TransformerConfigurationException e) {
			return false;
		}
		// adds proper formating
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        DOMSource source = new DOMSource(p_document);
        StreamResult result = new StreamResult(new File(m_path));
        try {
			transformer.transform(source, result);
		} catch (TransformerException e) {
			return false;
		}

         return true;
	}

	@Override
	public String toString()
	{
		return "ConfigurationXMLLoaderFile "+ m_path;
	}
}
