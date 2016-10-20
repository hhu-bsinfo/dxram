
package de.hhu.bsinfo.utils;

/**
 * Class for bundling two attributes
 * @author Stefan Nothaas
 * @param <T>
 *            first attribute
 * @param <K>
 *            second attribute
 */
public class Pair<T, K> {
	public T m_first;
	public K m_second;

	/**
	 * Default constructor
	 */
	public Pair() {

	}

	/**
	 * Creates an instance of Pair
	 * @param p_first
	 *            first attribute
	 * @param p_second
	 *            second attribute
	 */
	public Pair(final T p_first, final K p_second) {
		m_first = p_first;
		m_second = p_second;
	}

	/**
	 * Returns the first attribute
	 * @return the first attribute
	 */
	public T first() {
		return m_first;
	}

	/**
	 * Returns the second attribute
	 * @return the second attribute
	 */
	public K second() {
		return m_second;
	}
}
