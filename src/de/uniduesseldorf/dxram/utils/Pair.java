package de.uniduesseldorf.dxram.utils;

public class Pair<T, K> {
	private T m_first;
	private K m_second;
	
	public Pair(T p_first, K p_second)
	{
		m_first = p_first;
		m_second = p_second;
	}
	
	public T first()
	{
		return m_first;
	}
	
	public K second()
	{
		return m_second;
	}
}
