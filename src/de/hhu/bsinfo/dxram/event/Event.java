package de.hhu.bsinfo.dxram.event;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;

/**
 * Base class for events that can be fired within DXRAM.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public abstract class Event {
	private Class<? extends DXRAMComponent> m_sourceClass;
	
	/**
	 * Constructor
	 * @param p_sourceClass Source class this event originates from.
	 */
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
