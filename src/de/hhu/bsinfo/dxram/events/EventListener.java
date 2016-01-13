
package de.hhu.bsinfo.dxram.events;

/**
 * Methods for reacting to events
 * @param <T>
 *            the class of the events
 * @author Florian Klein
 *         09.03.2012
 */
public interface EventListener<T extends AbstractEvent> {

	// Methods
	/**
	 * Reacts on an Event
	 * @param p_event
	 *            the Event
	 */
	void triggerEvent(T p_event);

	// Classes
	/**
	 * Adapter for the EventListener
	 * @author Florian Klein
	 *         09.03.2012
	 * @param <T>
	 *            the abstract event
	 */
	class EventListenerAdapter<T extends AbstractEvent> implements EventListener<T> {

		// Constructors
		/**
		 * Creates an instance of EventListenerAdapter
		 */
		public EventListenerAdapter() {}

		// Methods
		/**
		 * Reacts on an Event
		 * @param p_event
		 *            the Event
		 */
		@Override
		public void triggerEvent(final T p_event) {}

	}

}
