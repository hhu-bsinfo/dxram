
package de.hhu.bsinfo.dxram.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.menet.TaskExecutor;

/**
 * Node local event system to notify other listening components about
 * something specified that happened.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class EventComponent extends AbstractDXRAMComponent {

	private LoggerComponent m_logger;

	private Map<String, ArrayList<EventListener<? extends AbstractEvent>>> m_eventListener = new HashMap<>();
	private TaskExecutor m_executor;

	/**
	 * Constructor
	 * @param p_priorityInit
	 *            Priority for initialization of this component.
	 *            When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown
	 *            Priority for shutting down this component.
	 *            When choosing the order, consider component dependencies here.
	 */
	public EventComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Register a listener to listen to specific event.
	 * @param <T>
	 *            Type of the event to listen to
	 * @param p_listener
	 *            Listener to register.
	 * @param p_class
	 *            Event to listen to.
	 */
	public <T extends AbstractEvent> void registerListener(final EventListener<T> p_listener, final Class<T> p_class) {
		ArrayList<EventListener<?>> listeners = m_eventListener.get(p_class.getName());
		if (listeners == null) {
			listeners = new ArrayList<EventListener<?>>();
			m_eventListener.put(p_class.getName(), listeners);
		}

		listeners.add(p_listener);
		// #if LOGGER >= DEBUG
		// // m_logger.debug(getClass(),
				// // "Registered listener " + p_listener.getClass().getName() + " for event " + p_class.getName());
		// #endif /* LOGGER >= DEBUG */
	}

	/**
	 * Fire an event.
	 * @param <T>
	 *            Type of event to fire.
	 * @param p_event
	 *            Event to fire.
	 */
	public <T extends AbstractEvent> void fireEvent(final T p_event) {
		// #if LOGGER == TRACE
		// // m_logger.trace(getClass(), "Event fired: " + p_event);
		// #endif /* LOGGER == TRACE */

		ArrayList<EventListener<?>> listeners = m_eventListener.get(p_event.getClass().getName());
		if (listeners != null) {
			FireEvent<T> task = new FireEvent<T>(p_event, listeners);

			if (m_executor != null) {
				m_executor.execute(task);
			} else {
				task.run();
			}
		}
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(EventConfigurationValues.Component.USE_EXECUTOR);
		p_settings.setDefaultValue(EventConfigurationValues.Component.THREAD_COUNT);
	}

	@Override
	protected boolean initComponent(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);

		if (p_settings.getValue(EventConfigurationValues.Component.USE_EXECUTOR)) {
			final int threadCount = p_settings.getValue(EventConfigurationValues.Component.THREAD_COUNT);
			// #if LOGGER >= INFO
			m_logger.info(getClass().getSimpleName(), "EventExecutor: Initialising " + threadCount + " threads");
			// #endif /* LOGGER >= INFO */
			m_executor = new TaskExecutor("EventExecutor", threadCount);
		}

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		if (m_executor != null) {
			m_executor.shutdown();
			try {
				m_executor.awaitTermination();
				// #if LOGGER >= INFO
				m_logger.info(getClass().getSimpleName(), "Shutdown of EventExecutor successful.");
				// #endif /* LOGGER >= INFO */
			} catch (final InterruptedException e) {
				// #if LOGGER >= WARN
				m_logger.warn(getClass().getSimpleName(),
						"Could not wait for event executor thread pool to finish. Interrupted.");
				// #endif /* LOGGER >= WARN */
			}
			m_executor = null;
		}

		return true;
	}

	/**
	 * Wrapper class to execute the firing of an event i.e. the calling
	 * of the listeners with the event fired as parameter in a separate thread.
	 * @param <T>
	 *            Type of the event.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
	 */
	private static class FireEvent<T extends AbstractEvent> implements Runnable {

		private AbstractEvent m_event;
		private ArrayList<EventListener<?>> m_listener;

		/**
		 * Constructor
		 * @param p_event
		 *            Event to fire.
		 * @param p_listener
		 *            List of listeners to receive the event.
		 */
		FireEvent(final T p_event, final ArrayList<EventListener<?>> p_listener) {
			m_event = p_event;
			m_listener = p_listener;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			for (EventListener<?> listener : m_listener) {
				((EventListener<T>) listener).eventTriggered((T) m_event);
			}
		}
	}

}
