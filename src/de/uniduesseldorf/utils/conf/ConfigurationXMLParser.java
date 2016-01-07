package de.uniduesseldorf.utils.conf;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import de.uniduesseldorf.utils.conf.Configuration;
import de.uniduesseldorf.utils.conf.ConfigurationException;

public class ConfigurationXMLParser implements ConfigurationParser
{
	private static final String ROOT_ELEMENT = "conf";
	private static final String ATTR_KEY_ID = "__id";
	private static final String ATTR_KEY_TYPE = "__type";
	private static final String ATTR_KEY_UNIT = "__unit";
	
	private static final String TYPE_STR = "str";
	private static final String TYPE_BYTE = "byte";
	private static final String TYPE_SHORT = "short";
	private static final String TYPE_INT = "int";
	private static final String TYPE_LONG = "long";
	private static final String TYPE_FLOAT = "float";
	private static final String TYPE_DOUBLE = "double";
	private static final String TYPE_BOOLEAN = "bool";
	private static final String TYPE_BOOLEAN_2 = "boolean";
	
	private ConfigurationXMLLoader m_loader;
	
	public ConfigurationXMLParser(final ConfigurationXMLLoader p_loader)
	{
		m_loader = p_loader;
	}
	
	@Override
	public void readConfiguration(final Configuration p_configuration) throws ConfigurationException {
		Document document = m_loader.load();
		if (document == null)
			throw new ConfigurationException("Loading configuration " + p_configuration.getName() + " failed");
		
		parseXML(document.getDocumentElement(), p_configuration);
	}

	@Override
	public void writeConfiguration(final Configuration p_configuration) throws ConfigurationException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		try {
			builder = factory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
		}
		Document document = builder.newDocument();
		generateXML(document, p_configuration);
		
		if (!m_loader.save(document))
			throw new ConfigurationException("Saving configuration " + p_configuration.getName() + " failed.");
	}

	private void parseXML(final Node p_root, final Configuration p_configuration) throws ConfigurationException {		
		NodeList children = p_root.getChildNodes();
		for (int i = 0; i < children.getLength(); i++)
		{
			Node rootChild = children.item(i);
			// support multiple root nodes
			if (rootChild.getNodeName().equals(ROOT_ELEMENT))
			{
				// iterate children of root node
				NodeList childrenOfRoot = rootChild.getChildNodes();
				for (int j = 0; j < childrenOfRoot.getLength(); j++)
				{
					Node confEntry = childrenOfRoot.item(j);
					parseChildren(confEntry, p_configuration, "");
				}
			}
		}
	}
	
	private void parseChildren(final Node p_parent, final Configuration p_configuration, final String p_key) throws ConfigurationException
	{
		String key = p_key;
		
		// no leafs, return
		if (p_parent == null)
			return;
		
		// extend path
		key += Configuration.KEY_SEQ_SEPARATOR + p_parent.getNodeValue();
		
		// only leafs are allowed to have attributes
		if (p_parent.hasAttributes())
		{
			// got leaf
			Object value = null;
			int index = 0;
			long unitConversionFactor = 1;
			
			NamedNodeMap attributes = p_parent.getAttributes();
			Node attrIndex = attributes.getNamedItem(ATTR_KEY_ID);
			Node attrType = attributes.getNamedItem(ATTR_KEY_TYPE);
			Node attrUnit = attributes.getNamedItem(ATTR_KEY_UNIT);
			
			// if id is missing, 0 is assumed
			if (attrIndex != null) {
				index = Integer.parseInt(attrIndex.getNodeValue());
			} else {
				index = 0;
			}
			
			// prepare unit conversion
			// this does only apply to integer numbers
			if (attrUnit != null) {
				if (attrUnit.getNodeValue().equals("kb")) {
					unitConversionFactor = 1024;
				} else if (attrUnit.getNodeValue().equals("mb")) {
					unitConversionFactor = 1024 * 1024;
				} else if (attrUnit.getNodeValue().equals("gb")) {
					unitConversionFactor = 1024 * 1024 * 1024;
				} else {
					unitConversionFactor = 1;
				}
			}
			
			// if no type specified, we assume a string
			if (attrType == null || attrType.getNodeValue().equals(TYPE_STR)) {
				value = new String(p_parent.getNodeValue());
			} else if (attrType.getNodeValue().equals(TYPE_BYTE)) {
				value = Byte.parseByte(p_parent.getNodeValue()) * unitConversionFactor;
			} else if (attrType.getNodeValue().equals(TYPE_SHORT)) {
				value = Short.parseShort(p_parent.getNodeValue()) * unitConversionFactor;
			} else if (attrType.getNodeValue().equals(TYPE_INT)) {
				value = Integer.parseInt(p_parent.getNodeValue()) * unitConversionFactor;
			} else if (attrType.getNodeValue().equals(TYPE_LONG)) {
				value = Long.parseLong(p_parent.getNodeValue()) * unitConversionFactor;
			} else if (attrType.getNodeValue().equals(TYPE_FLOAT)) {
				value = Float.parseFloat(p_parent.getNodeValue());
			} else if (attrType.getNodeValue().equals(TYPE_DOUBLE)) {
				value = Double.parseDouble(p_parent.getNodeValue());
			} else if (attrType.getNodeValue().equals(TYPE_BOOLEAN) || attrType.getNodeValue().equals(TYPE_BOOLEAN_2)) {
				// allow 0 and 1 as well
				if (p_parent.getNodeValue().equals("0")) {
					value = new Boolean(false);
				} else if (p_parent.getNodeValue().equals("1")) {
					value = new Boolean(true);
				} else {
					value = Boolean.parseBoolean(p_parent.getNodeValue());
				}
			} 
			
			// add the value and do not replace existing values
			// i.e. if same index is available multiple times, only the first one is used
			p_configuration.AddValue(key, index, value, false);
		}
		else
		{
			// got inner node, continue to walk down the hierarchy
			NodeList children = p_parent.getChildNodes();
			for (int i = 0; i < children.getLength(); i++)
			{
				parseChildren(children.item(i), p_configuration, key);
			}
		}
	}
	
	private void generateXML(final Document p_document, final Configuration p_configuration) throws ConfigurationException {
		
		Element confRoot = p_document.createElement(ROOT_ELEMENT);
		p_document.appendChild(confRoot);
		
		generateChildren(p_document, confRoot, p_configuration);
	}
	
	
	// if we got one index for the path -> generate full hierarchy with index 0 and add value
	// otherwise:
	// 1. remove leaf and check if the path minus leaf already exists (multiple possible -> list)
	// 	  not, empty list: fully create it as inner leafs only -> return parent of leaf
	// 	  exists, list got items: iterate possible parent of leaf and check get any child from that and check against attribute id
	//			id matches: add leaf to the same parent with same id and abort iteration
	//			id does not match any possible parent: create new parent and add new leaf to it with id
	//
	private void generateChildren(final Document p_document, final Element p_root, final Configuration p_configuration) {
		for (Entry<String, Map<Integer, Object>> it : p_configuration.m_parameters.entrySet())
		{
			// two possibilities:
			// 1. single item: we can create an entry without further processing
			// 2. multiple indexed items: we have to work on some kind of lists
			if (it.getValue().size() == 1)
			{
				// it's just one element, but easier to get it like that
				for (Entry<Integer, Object> it2 : it.getValue().entrySet())
				{
					Element leaf = generateHierarchy(p_document, p_root, it.getKey());
					setLeaf(p_document, leaf, it2.getValue(), it2.getKey());
				}
			}
			else
			{
				for (Entry<Integer, Object> it2 : it.getValue().entrySet())
				{
					
				}
			}
			
			for (Entry<Integer, Object> it2 : it.getValue().entrySet())
			{
				Element elem = generateHierarchy(p_document, p_root, it.getKey(), it2.getKey());
				Object value = it2.getValue();
				
				
				

			}
		}
	}
	
	private ArrayList<Element> getParentsListOfLeaf(final Element p_root, final String p_path)
	{
		ArrayList<Element> parents = new ArrayList<Element>();
		String[] pathTokens = p_path.split(Configuration.KEY_SEQ_SEPARATOR);
		
		// drop first token, it is empty (-1)
		if (pathTokens.length - 1 == 1)
		{
			// leaf on root
			parents.add(p_root);
		}
		else
		{
			
		}
		
		return parents;
	}
	
	private ArrayList<Element> getParentsOfLeaf(final Element p_root, final String p_path)
	{
		ArrayList<Element> parents = new ArrayList<Element>();
		String[] pathTokens = p_path.split(Configuration.KEY_SEQ_SEPARATOR);
		
		// drop first token, it is empty (-1)
		if (pathTokens.length - 1 == 1)
		{
			// leaf on root
			parents.add(p_root);
		}
		else
		{
			// crate path to parent of parent of leaf
			String pathParent = new String("");
			for (int i = 1; i < pathTokens.length - 1; i++) 
			{
				Element next = null;
				
				
				
				// check if element already exists (inner nodes)
				NodeList nodes = cur.getChildNodes();
				for (int j = 0; j < nodes.getLength(); j++) {
					Node child = nodes.item(j);
					if (child.getNodeType() == Element.ELEMENT_NODE)
					{
						Element elem = (Element) child;
						if (elem.getTagName().equals(items[i])) {
							next = (Element) child;
							break;
						}
					}
				}
				
				// create if not exists
				if (next == null) {
					next = p_document.createElement(items[i]);
					cur.appendChild(next);
				}
				
				cur = next;
			}
			
			
			
			generateHierarchy()
			
			// leaf further down the tree
			// iterate to parent which might contain multiple parents of the leaf
			// i = 1: ignore empty element created by split due to leading / for root
			for (int i = 1; i < pathTokens.length; i++)
			{
				Element next = null;
				
				// check if element already exists (inner nodes)
				NodeList nodes = cur.getChildNodes();
				for (int j = 0; j < nodes.getLength(); j++) {
					Node child = nodes.item(j);
					if (child.getNodeType() == Element.ELEMENT_NODE)
					{
						Element elem = (Element) child;
						if (elem.getTagName().equals(items[i])) {
							next = (Element) child;
							break;
						}
					}
				}
				
				// create if not exists
				if (next == null) {
					next = p_document.createElement(items[i]);
					cur.appendChild(next);
				}
				
				cur = next;
			}
		}
		
		return parents;
	}
	
	private void setLeaf(final Document p_document, final Element p_element, final Object p_value, final int p_index)
	{
		p_element.setAttribute(ATTR_KEY_ID, Integer.toString(p_index));
		
		if (p_value instanceof String) {
			p_element.setAttribute(ATTR_KEY_TYPE, TYPE_STR);
			p_element.appendChild(p_document.createTextNode((String) p_value));
		} else if (p_value instanceof Byte) {
			p_element.setAttribute(ATTR_KEY_TYPE, TYPE_BYTE);
			p_element.appendChild(p_document.createTextNode(((Byte) p_value).toString()));
		} else if (p_value instanceof Short) {
			p_element.setAttribute(ATTR_KEY_TYPE, TYPE_SHORT);
			p_element.appendChild(p_document.createTextNode(((Short) p_value).toString()));
		} else if (p_value instanceof Integer) {
			p_element.setAttribute(ATTR_KEY_TYPE, TYPE_INT);
			p_element.appendChild(p_document.createTextNode(((Integer) p_value).toString()));
		} else if (p_value instanceof Long) {
			p_element.setAttribute(ATTR_KEY_TYPE, TYPE_LONG);
			p_element.appendChild(p_document.createTextNode(((Long) p_value).toString()));
		} else if (p_value instanceof Float) {
			p_element.setAttribute(ATTR_KEY_TYPE, TYPE_FLOAT);
			p_element.appendChild(p_document.createTextNode(((Float) p_value).toString()));
		} else if (p_value instanceof Double) {
			p_element.setAttribute(ATTR_KEY_TYPE, TYPE_DOUBLE);
			p_element.appendChild(p_document.createTextNode(((Double) p_value).toString()));
		} else if (p_value instanceof Boolean) {
			p_element.setAttribute(ATTR_KEY_TYPE, TYPE_BOOLEAN);
			p_element.appendChild(p_document.createTextNode(((Boolean) p_value).toString()));
		} else {
			// not supported, ignoring
		}
	}
	

	
	private Element generateEntry(final Document p_document, final Element p_root, final String p_path, final int p_index)
	{
		String[] items = p_path.split(Configuration.KEY_SEQ_SEPARATOR);
		Element leafParent = p_root;
		
		// i = 1: ignore empty element created by split due to leading / for root
		// generate or get existing inner nodes up the the node before the leaf
		for (int i = 1; i < items.length - 1; i++)
		{
			Element cur = null;
			
			NodeList nodes = leafParent.getChildNodes();
			for (int j = 0; j < nodes.getLength(); j++) {
				Node child = nodes.item(j);
				if (child.getNodeType() == Element.ELEMENT_NODE)
				{
					Element elem = (Element) child;
					if (elem.getTagName().equals(items[i])) {
						cur = (Element) child;
						break;
					}
				}
			}
			
			// check if this is a parent of a leaf
			if (i == items.length - 2)
			{
				
			}
			else
			{
				// lower inner node
				// create new inner node if not available
				if (cur == null) {
					cur = p_document.createElement(items[i]);
					leafParent.appendChild(cur);
				}
			}
			

			
			leafParent = cur;
		}
		
		if (leafParent.getParentNode() != null)
		{
			// adding leaf further down the tree
			// check if multiple inner nodes with the same name exist
			
		}
		else
		{
			// adding leaf to root
			
		}
		
		Element elem = p_document.createElement(items[items.length - 1]);
		elem.setAttribute(ATTR_KEY_ID, (new Integer(p_index)).toString());
		leafParent.appendChild(elem);
	
		return null;
	}
	
	// generates a hierarchy of inner nodes and 
	// reuses existing ones i.e. does not create duplicate inner nodes
	private Element generateHierarchy(final Document p_document, final Element p_root, final String p_path) 
	{
		String[] items = p_path.split(Configuration.KEY_SEQ_SEPARATOR);
		Element cur = p_root;
		
		// i = 1: ignore empty element created by split due to leading / for root
		for (int i = 1; i < items.length; i++)
		{
			Element next = null;
			
			// check if element already exists (inner nodes)
			NodeList nodes = cur.getChildNodes();
			for (int j = 0; j < nodes.getLength(); j++) {
				Node child = nodes.item(j);
				if (child.getNodeType() == Element.ELEMENT_NODE)
				{
					Element elem = (Element) child;
					if (elem.getTagName().equals(items[i])) {
						next = (Element) child;
						break;
					}
				}
			}
			
			// create if not exists
			if (next == null) {
				next = p_document.createElement(items[i]);
				cur.appendChild(next);
			}
			
			cur = next;
		}
		
		return cur;
	}
}
