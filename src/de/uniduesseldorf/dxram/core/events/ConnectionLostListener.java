
package de.uniduesseldorf.dxram.core.events;

import de.uniduesseldorf.dxram.core.events.ConnectionLostListener.ConnectionLostEvent;

/**
 * Methods for reacting to ConnectionLostEvents
 * @author Florian Klein
 *         01.08.2012
 */
public interface ConnectionLostListener extends EventListener<ConnectionLostEvent> {

	// Methods
	@Override
	void triggerEvent(ConnectionLostEvent p_event);

	// Classes
	/**
	 * Event will be triggered, if the connection to a node is lost
	 * @author Florian Klein
	 *         09.03.2012
	 */
	public static class ConnectionLostEvent extends AbstractEvent {

		// Constructors
		/**
		 * Creates an instance of ConnectionLostEvent
		 * @param p_source
		 *            the NodeID of the event source
		 */
		public ConnectionLostEvent(final short p_source) {
			super(p_source);
		}

		// Methods
		@Override
		protected final String infoString() {
			return "";
		}

	}

}
