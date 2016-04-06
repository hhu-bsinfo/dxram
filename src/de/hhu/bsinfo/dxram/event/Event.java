package de.hhu.bsinfo.dxram.event;

/**
 * Base class for events that can be fired within DXRAM.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public abstract class Event {
	private String m_sourceClass;
	
	/**
	 * Constructor
	 * @param p_sourceClass Source class this event originates from.
	 */
	public Event(final String p_sourceClass)
	{
		m_sourceClass = p_sourceClass;
	}
	
	@Override
	public String toString()
	{
		return getClass().getSimpleName() + "[" + m_sourceClass + "]";
	}
}
