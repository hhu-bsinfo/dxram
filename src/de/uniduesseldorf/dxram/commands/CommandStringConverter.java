package de.uniduesseldorf.dxram.commands;

import java.util.HashMap;
import java.util.Map;

/**
 * Methods for converting Strings into integers
 * @author Kevin Beineke
 *         12.08.2015
 */
public final class CommandStringConverter {
	
	// Attributes
	private static Map<String, Short> m_commandMap;

	// Constructors
	/**
	 * Creates an instance of StringConverter
	 */
	private CommandStringConverter() {}

	static {
		m_commandMap = new HashMap<String, Short>();
		m_commandMap.put("list", (short) 1);
		m_commandMap.put("migrate", (short) 2);
	}

	// Methods
	/**
	 * Converts a String into an integer
	 * @param p_command
	 *            the String
	 * @return the short, -1 = unknown command
	 */
	public static short convert(final String p_command) {
		short ret = -1;
		Short type;

		type = m_commandMap.get(p_command);
		if (type != null) {
			ret = type.shortValue();
		}

		return ret;
	}

}
