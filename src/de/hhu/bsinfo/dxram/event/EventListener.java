package de.hhu.bsinfo.dxram.event;

/**
 * Listener interface for an event listener.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 * @param <T> Event to listen to.
 */
public interface EventListener<T extends AbstractEvent> {
	/**
	 * Called by the event system if the specified event was fired.
	 * @param p_event Event that was fired.
	 */
	void eventTriggered(final T p_event);
}
