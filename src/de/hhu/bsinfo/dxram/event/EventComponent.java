/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.utils.event.EventInterface;

/**
 * Node local event system to notify other listening components about
 * something specified that happened.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class EventComponent extends AbstractDXRAMComponent implements EventInterface {

    private static final Logger LOGGER = LogManager.getFormatterLogger(EventComponent.class.getSimpleName());

    // configuration values
    @Expose
    private boolean m_useExecutor = true;
    @Expose
    private int m_threadCount = 1;

    // dependent components

    // private state
    private Map<String, ArrayList<EventListener<? extends AbstractEvent>>> m_eventListener = new HashMap<>();
    private TaskExecutor m_executor;

    /**
     * Constructor
     */
    public EventComponent() {
        super(DXRAMComponentOrder.Init.EVENT, DXRAMComponentOrder.Shutdown.EVENT);
    }

    /**
     * Register a listener to listen to specific event.
     *
     * @param <T>
     *     Type of the event to listen to
     * @param p_listener
     *     Listener to register.
     * @param p_class
     *     Event to listen to.
     */
    public <T extends AbstractEvent> void registerListener(final EventListener<T> p_listener, final Class<?> p_class) {
        ArrayList<EventListener<?>> listeners = m_eventListener.get(p_class.getName());
        if (listeners == null) {
            listeners = new ArrayList<>();
            m_eventListener.put(p_class.getName(), listeners);
        }

        listeners.add(p_listener);
        // #if LOGGER >= DEBUG
        LOGGER.debug("Registered listener %s for event %s", p_listener.getClass().getName(), p_class.getName());
        // #endif /* LOGGER >= DEBUG */
    }

    /**
     * Fire an event.
     *
     * @param <T>
     *     Type of event to fire.
     * @param p_event
     *     Event to fire.
     */
    @Override
    public <T extends AbstractEvent> void fireEvent(final T p_event) {
        // #if LOGGER == TRACE
        LOGGER.trace("Event fired: %s", p_event);
        // #endif /* LOGGER == TRACE */

        ArrayList<EventListener<?>> listeners = m_eventListener.get(p_event.getClass().getName());
        if (listeners != null) {
            FireEvent<T> task = new FireEvent<>(p_event, listeners);

            if (m_executor != null) {
                m_executor.execute(task);
            } else {
                task.run();
            }
        }
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        // no dependencies
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        if (m_useExecutor) {
            // #if LOGGER >= INFO
            LOGGER.info("EventExecutor: Initialising %d threads", m_threadCount);
            // #endif /* LOGGER >= INFO */
            m_executor = new TaskExecutor("EventExecutor", m_threadCount);
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
                LOGGER.info("Shutdown of EventExecutor successful");
                // #endif /* LOGGER >= INFO */
            } catch (final InterruptedException e) {
                // #if LOGGER >= WARN
                LOGGER.warn("Could not wait for event executor thread pool to finish. Interrupted");
                // #endif /* LOGGER >= WARN */
            }
            m_executor = null;
        }

        return true;
    }

    /**
     * Wrapper class to execute the firing of an event i.e. the calling
     * of the listeners with the event fired as parameter in a separate thread.
     *
     * @param <T>
     *     Type of the event.
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
     */
    private static class FireEvent<T extends AbstractEvent> implements Runnable {

        private AbstractEvent m_event;
        private ArrayList<EventListener<?>> m_listener;

        /**
         * Constructor
         *
         * @param p_event
         *     Event to fire.
         * @param p_listener
         *     List of listeners to receive the event.
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
