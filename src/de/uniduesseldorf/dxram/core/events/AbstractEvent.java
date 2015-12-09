
package de.uniduesseldorf.dxram.core.events;

import de.uniduesseldorf.dxram.core.api.nodeconfig.NodeID;

/**
 * Respresents an Event, which can be triggered
 * @author Florian Klein
 *         09.03.2012
 */
public abstract class AbstractEvent {

	// Attributes
	private short m_source;

	// Constructors
	/**
	 * Creates an instance of Event
	 * @param p_source
	 *            the NodeID of the event source
	 */
	public AbstractEvent(final short p_source) {
		NodeID.check(p_source);

		m_source = p_source;
	}

	// Getters
	/**
	 * Get the event source
	 * @return the event source
	 */
	public final short getSource() {
		return m_source;
	}

	// Methods
	@Override
	public final String toString() {
		return this.getClass().getSimpleName() + "[" + m_source + ", " + infoString() + "]";
	}

	/**
	 * Get a String with further information about the event
	 * @return the String with further information about the event
	 */
	protected abstract String infoString();

}
