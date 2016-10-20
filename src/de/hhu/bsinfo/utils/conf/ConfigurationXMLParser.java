
package de.hhu.bsinfo.utils.conf;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import de.hhu.bsinfo.utils.reflect.dt.DataTypeParser;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserBool;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserBoolean;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserByte;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserDouble;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserFloat;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserInt;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserLong;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserShort;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserString;
import de.hhu.bsinfo.utils.reflect.unit.UnitConverter;
import de.hhu.bsinfo.utils.reflect.unit.UnitConverterGBToByte;
import de.hhu.bsinfo.utils.reflect.unit.UnitConverterKBToByte;
import de.hhu.bsinfo.utils.reflect.unit.UnitConverterMBToByte;

public class ConfigurationXMLParser implements ConfigurationParser {
	private static final String ROOT_ELEMENT = "conf";
	private static final String ATTR_KEY_ID = "__id";
	private static final String ATTR_KEY_TYPE = "__type";
	private static final String ATTR_KEY_UNIT = "__unit";

	private ConfigurationXMLLoader m_loader = null;
	private Map<String, DataTypeParser> m_dataTypeParsers = new HashMap<String, DataTypeParser>();
	private Map<String, UnitConverter> m_unitConverters = new HashMap<String, UnitConverter>();

	public ConfigurationXMLParser(final ConfigurationXMLLoader p_loader) {
		m_loader = p_loader;

		// add default type parsers
		addDataTypeParser(new DataTypeParserString());
		addDataTypeParser(new DataTypeParserByte());
		addDataTypeParser(new DataTypeParserShort());
		addDataTypeParser(new DataTypeParserInt());
		addDataTypeParser(new DataTypeParserLong());
		addDataTypeParser(new DataTypeParserFloat());
		addDataTypeParser(new DataTypeParserDouble());
		addDataTypeParser(new DataTypeParserBool());
		addDataTypeParser(new DataTypeParserBoolean());

		// add default unit converters
		addUnitConverter(new UnitConverterKBToByte());
		addUnitConverter(new UnitConverterMBToByte());
		addUnitConverter(new UnitConverterGBToByte());
	}

	public boolean addDataTypeParser(final DataTypeParser p_parser) {
		return m_dataTypeParsers.put(p_parser.getTypeIdentifer(), p_parser) == null;
	}

	public boolean addUnitConverter(final UnitConverter p_converter) {
		return m_unitConverters.put(p_converter.getUnitIdentifier(), p_converter) == null;
	}

	@Override
	public void readConfiguration(final Configuration p_configuration) throws ConfigurationException {
		Document document = m_loader.load();
		if (document == null) {
			throw new ConfigurationException("Loading configuration " + p_configuration.getName() + " failed");
		}

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

		if (!m_loader.save(document)) {
			throw new ConfigurationException("Saving configuration " + p_configuration.getName() + " failed.");
		}
	}

	private void parseXML(final Node p_root, final Configuration p_configuration) throws ConfigurationException {

		if (p_root.getNodeName().equals(ROOT_ELEMENT)) {
			// iterate children of root node
			NodeList childrenOfRoot = p_root.getChildNodes();
			for (int j = 0; j < childrenOfRoot.getLength(); j++) {
				Node confEntry = childrenOfRoot.item(j);
				if (confEntry.getNodeType() == Element.ELEMENT_NODE) {
					parseChildren((Element) confEntry, p_configuration, "");
				}
			}
		}
	}

	private void parseChildren(final Element p_parent, final Configuration p_configuration, final String p_key)
			throws ConfigurationException {
		String key = new String(p_key);

		// no leafs, return
		if (p_parent == null) {
			return;
		}

		// extend path
		key += Configuration.KEY_SEQ_SEPARATOR + p_parent.getTagName();

		// only leafs are allowed to have attributes
		if (p_parent.hasAttributes()) {
			// got leaf
			Object value = null;
			int index = 0;

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

			if (attrType != null) {
				DataTypeParser parser = m_dataTypeParsers.get(attrType.getNodeValue());
				if (parser != null) {
					value = parser.parse(p_parent.getTextContent());

					// check for unit conversion, attribute optional
					if (attrUnit != null) {
						UnitConverter unitConverter = m_unitConverters.get(attrUnit.getNodeValue());
						if (unitConverter != null) {
							value = unitConverter.convert(value);
						}
					}

					// add the value and do not replace existing values
					// i.e. if same index is available multiple times, only the first one is used
					p_configuration.addValue(key, index, value, false);
				}
				// no parser to support, ignore
			}

			// missing type, ignore
		} else {
			// got inner node, continue to walk down the hierarchy
			NodeList children = p_parent.getChildNodes();
			for (int i = 0; i < children.getLength(); i++) {
				Node child = children.item(i);
				if (child.getNodeType() == Element.ELEMENT_NODE) {
					parseChildren((Element) child, p_configuration, key);
				}
			}
		}
	}


	/**
	 * Generates the xml document with the aid of the Configuration object.
	 * @param p_document XML Document
	 * @param p_configuration Configuration object
	 * @throws ConfigurationException
     */
	private void generateXML(final Document p_document, final Configuration p_configuration) throws ConfigurationException {

		Element confRoot = p_document.createElement(ROOT_ELEMENT);
		p_document.appendChild(confRoot);

		generateChildren(p_document, confRoot, p_configuration);

	}

	/**
	 * Appends xml document for each configuration in the Configuration object.
	 * @param p_document
	 * @param p_root
	 * @param p_configuration
     */
	private void generateChildren(final Document p_document, final Element p_root, final Configuration p_configuration) {
		for (Map.Entry<String, Map<Integer, Object>> it : p_configuration.m_parameters.entrySet()) {

			//two possibilities:
			// 1. single item: we can create an entry without processing
			// 2. multiple indexd items: we have to work on some kind of lists
			if (it.getValue().size() == 1) {
				//it's just one element, but easier to get it like that
				for (Map.Entry<Integer, Object> it2 : it.getValue().entrySet()) {
					Element leaf = generateHierarchy(p_document, p_root, it.getKey(), 0);
					setLeaf(p_document, leaf, it2.getValue(), it2.getKey());
				}
			} else {
				for (Map.Entry<Integer, Object> it2 : it.getValue().entrySet()) {
					Element element = generateHierarchy(p_document, p_root, it.getKey(), it2.getKey());
					setLeaf(p_document, element, it2.getValue(), it2.getKey());
				}
			}

		}
	}

	/**
	 * Generates a hierarchy for the element that will be appended on basis of p_path
	 * @param p_document XML document
	 * @param p_root root
	 * @param p_path path of the element
	 * @param p_id id
     * @return
     */
	private Element generateHierarchy(final Document p_document, final Element p_root, final String p_path, final int p_id) {
		String[] items = p_path.split(Configuration.KEY_SEQ_SEPARATOR);
		Element cur = p_root;


		for (int i = 1; i < items.length; i++) {
			Element next = null;

			NodeList nodes = cur.getChildNodes();
			for (int j = 0; j < nodes.getLength(); j++) {
				Node child = nodes.item(j);
				if (child.getNodeType() == Element.ELEMENT_NODE) {
					Element element = (Element) child;
					if (element.getTagName().equals(items[i])) { // path is up to now equal
						if (element.hasAttribute(ATTR_KEY_ID)) { // current element has an id attribute
							int elementValue = Integer.parseInt(element.getAttribute(ATTR_KEY_ID));
							if (elementValue != p_id ) { // id is different so append child on the prior node
								next = p_document.createElement(items[i - 1]);
								cur.getParentNode().appendChild(next);
								i = i - 1;
							}
						} else { // current element has not an id attribute -> check if childs have an id attribute with the a
							NodeList nodes2 = element.getChildNodes();
							for(int k=0; k<nodes2.getLength(); k++) {
								Node child2 = nodes2.item(k);
								if(child2.getNodeType() == Element.ELEMENT_NODE) {
									Element element2 = (Element) child2;
									if(element2.hasAttribute(ATTR_KEY_ID)) {
										int elmtId = Integer.parseInt(element2.getAttribute(ATTR_KEY_ID));
										if(elmtId == p_id) {
											next = (Element) child;
											break;
										} else continue;
									} else {
										next = (Element) child;
										break;
									}
								}
							}

						}
					}
				}
			}


			if (next == null) {
				next = p_document.createElement(items[i]);
				cur.appendChild(next);
			}
			cur = next;
		}

		return cur;
	}


	/**
	 * Appends the leaf as a child in the xml document and assignes the value of the leaf.
	 * @param p_document XML Document
	 * @param p_element leaf
	 * @param p_value Value of Leaf
     * @param p_index Index attribute
     */
	private void setLeaf(final Document p_document, final Element p_element, final Object p_value, final int p_index) {
		p_element.setAttribute(ATTR_KEY_ID, Integer.toString(p_index));

		if (p_value instanceof String) {
			p_element.setAttribute(ATTR_KEY_TYPE, "str");
			p_element.appendChild(p_document.createTextNode((String) p_value));
		} else if (p_value instanceof Byte) {
			p_element.setAttribute(ATTR_KEY_TYPE, "byte");
			p_element.appendChild(p_document.createTextNode(((Byte) p_value).toString()));
		} else if (p_value instanceof Short) {
			p_element.setAttribute(ATTR_KEY_TYPE, "short");
			p_element.appendChild(p_document.createTextNode(((Short) p_value).toString()));
		} else if (p_value instanceof Integer) {
			p_element.setAttribute(ATTR_KEY_TYPE, "int");
			p_element.appendChild(p_document.createTextNode(((Integer) p_value).toString()));
		} else if (p_value instanceof Long) {
			p_element.setAttribute(ATTR_KEY_TYPE, "long");
			p_element.appendChild(p_document.createTextNode(((Long) p_value).toString()));
		} else if (p_value instanceof Float) {
			p_element.setAttribute(ATTR_KEY_TYPE, "float");
			p_element.appendChild(p_document.createTextNode(((Float) p_value).toString()));
		} else if (p_value instanceof Double) {
			p_element.setAttribute(ATTR_KEY_TYPE, "double");
			p_element.appendChild(p_document.createTextNode(((Double) p_value).toString()));
		} else if (p_value instanceof Boolean) {
			p_element.setAttribute(ATTR_KEY_TYPE, "bool");
			p_element.appendChild(p_document.createTextNode(((Boolean) p_value).toString()));
		} else {
			//not supported, ignoring
		}
	}
}


