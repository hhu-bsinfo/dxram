package de.uniduesseldorf.dxram.core.engine;

public class DXRAMComponent 
{
	private DXRAMEngine m_engine;
	
	private String m_identifier;
	
	private int m_priorityInit;
	
	private int m_priorityShutdown;
	
	public DXRAMComponent(final String p_componentIdentifier, final int p_priorityInit, final int p_priorityShutdown)
	{
		m_identifier = p_componentIdentifier;
		m_priorityInit = p_priorityInit;
		m_priorityShutdown = p_priorityShutdown;
	}
	
	public String getIdentifier()
	{
		return m_identifier;
	}
	
	public int getPriorityInit()
	{
		return m_priorityInit;
	}
	
	public int getPriorityShutdown()
	{
		return m_priorityShutdown;
	}
	
	public boolean init(final DXRAMEngine p_engine)
	{
		m_engine = p_engine;
		
		
	}
}
