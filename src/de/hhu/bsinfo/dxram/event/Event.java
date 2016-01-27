package de.hhu.bsinfo.dxram.event;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;

public abstract class Event {
	private Class<? extends DXRAMComponent> m_sourceClass;
	
	public Event(final Class<? extends DXRAMComponent> p_sourceClass)
	{
		m_sourceClass = p_sourceClass;
	}
	
	@Override
	public String toString()
	{
		return getClass().getSimpleName() + "[" + m_sourceClass.getSimpleName() + "]";
	}
}
