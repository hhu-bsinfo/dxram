package de.uniduesseldorf.dxram.utils;

/** Minimal class to store two objects as one immutable pair.
 * 
 * @author Stefan Nothaas
 *
 * @param <T> First object of pair.
 * @param <K> Second object of pair.
 */
public class Pair<T, K> {
	private T m_first;
	private K m_second;
	
	/** Constructor
	 * 
	 * @param p_first First object.
	 * @param p_second Second object.
	 */
	public Pair(T p_first, K p_second)
	{
		m_first = p_first;
		m_second = p_second;
	}
	
	/** Get the first object.
	 * 
	 * @return First object.
	 */
	public T first()
	{
		return m_first;
	}
	
	/** Get the second object.
	 * 
	 * @return Second object.
	 */
	public K second()
	{
		return m_second;
	}
}
