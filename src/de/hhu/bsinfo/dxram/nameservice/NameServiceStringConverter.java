
package de.hhu.bsinfo.dxram.nameservice;

import java.util.HashMap;
import java.util.Map;

/**
 * Methods for converting Strings into integers
 * The character set is very limited. Make sure to use valid characters only to avoid undefined behaviour.
 * @author Kevin Beineke
 *         14.02.2014
 */
public final class NameServiceStringConverter {

	private String m_nameserviceType;
	private Map<Character, Integer> m_charMap = new HashMap<Character, Integer>();

	/**
	 * Creates an instance of StringConverter
	 * @param p_nameserviceType
	 *            Type of the string converter to use
	 */
	public NameServiceStringConverter(final String p_nameserviceType) {
		m_nameserviceType = p_nameserviceType;

		if (m_nameserviceType.equals("NAME")) {
			m_charMap = new HashMap<Character, Integer>();
			m_charMap.put('0', 1);
			m_charMap.put('1', 2);
			m_charMap.put('2', 3);
			m_charMap.put('3', 4);
			m_charMap.put('4', 5);
			m_charMap.put('5', 6);
			m_charMap.put('6', 7);
			m_charMap.put('7', 8);
			m_charMap.put('8', 9);
			m_charMap.put('9', 10);

			m_charMap.put('a', 11);
			m_charMap.put('b', 12);
			m_charMap.put('c', 13);
			m_charMap.put('d', 14);
			m_charMap.put('e', 15);
			m_charMap.put('f', 16);
			m_charMap.put('g', 17);
			m_charMap.put('h', 18);
			m_charMap.put('i', 19);
			m_charMap.put('j', 20);
			m_charMap.put('k', 21);
			m_charMap.put('l', 22);
			m_charMap.put('m', 23);
			m_charMap.put('n', 24);
			m_charMap.put('o', 25);
			m_charMap.put('p', 26);
			m_charMap.put('q', 27);
			m_charMap.put('r', 28);
			m_charMap.put('s', 29);
			m_charMap.put('t', 30);
			m_charMap.put('u', 31);
			m_charMap.put('v', 32);
			m_charMap.put('w', 33);
			m_charMap.put('x', 34);
			m_charMap.put('y', 35);
			m_charMap.put('z', 36);

			m_charMap.put('A', 37);
			m_charMap.put('B', 38);
			m_charMap.put('C', 39);
			m_charMap.put('D', 40);
			m_charMap.put('E', 41);
			m_charMap.put('F', 42);
			m_charMap.put('G', 43);
			m_charMap.put('H', 44);
			m_charMap.put('I', 45);
			m_charMap.put('J', 46);
			m_charMap.put('K', 47);
			m_charMap.put('L', 48);
			m_charMap.put('M', 49);
			m_charMap.put('N', 50);
			m_charMap.put('O', 51);
			m_charMap.put('P', 52);
			m_charMap.put('Q', 53);
			m_charMap.put('R', 54);
			m_charMap.put('S', 55);
			m_charMap.put('T', 56);
			m_charMap.put('U', 57);
			m_charMap.put('V', 58);
			m_charMap.put('W', 59);
			m_charMap.put('X', 60);
			m_charMap.put('Y', 61);
			m_charMap.put('Z', 62);

			m_charMap.put('-', 63);
		}
	}

	/**
	 * Converts a String into an integer. String length is limited to 5 chars.
	 * If the string is longer, all other characters are ignored.
	 * @param p_name
	 *            the String
	 * @return the integer
	 */
	public int convert(final String p_name) {
		int ret = 0;
		int value = 0;
		char[] chars;

		if (m_nameserviceType.equals("NAME")) {
			chars = p_name.toCharArray();
			for (int i = 0; i < 32 / 6 && i < chars.length; i++) {
				value = m_charMap.get(chars[i]);
				ret += value << i * 6;
			}
		} else {
			ret = Integer.parseInt(p_name);
		}

		return ret;
	}

}
