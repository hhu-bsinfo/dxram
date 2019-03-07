/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;

/**
 * Node local event system to notify other listening components about
 * something specified that happened.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = true, supportsPeer = true)
@AbstractDXRAMComponent.Attributes(priorityInit = DXRAMComponentOrder.Init.EVENT,
        priorityShutdown = DXRAMComponentOrder.Shutdown.EVENT)
public class EventComponent extends AbstractDXRAMComponent<EventComponentConfig> implements EventInterface {
    // private state
    private Map<String, ArrayList<EventListener<? extends AbstractEvent>>> m_eventListener = new HashMap<>();
    private TaskExecutor m_executor;

    /**
     * Register a listener to listen to specific event.
     *
     * @param <T>
     *         Type of the event to listen to
     * @param p_listener
     *         Listener to register.
     * @param p_class
     *         Event to listen to.
     */
    public <T extends AbstractEvent> void registerListener(final EventListener<T> p_listener, final Class<?> p_class) {
        ArrayList<EventListener<?>> listeners = m_eventListener.get(p_class.getName());
        if (listeners == null) {
            listeners = new ArrayList<>();
            m_eventListener.put(p_class.getName(), listeners);
        }

        listeners.add(p_listener);

        LOGGER.debug("Registered listener %s for event %s", p_listener.getClass().getName(), p_class.getName());

    }

    /**
     * Fire an event.
     *
     * @param <T>
     *         Type of event to fire.
     * @param p_event
     *         Event to fire.
     */
    @Override
    public <T extends AbstractEvent> void fireEvent(final T p_event) {

        LOGGER.trace("Event fired: %s", p_event);

        ArrayList<EventListener<?>> listeners = m_eventListener.get(p_event.getClass().getName());
        if (listeners != null) {
            FireEvent<T> task = new FireEvent<>(p_event, listeners);

            m_executor.execute(task);
        }
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        // no dependencies
    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        LOGGER.debug("EventExecutor: Initialising %d threads", getConfig().getThreadCount());

        m_executor = new TaskExecutor("EventExecutor", getConfig().getThreadCount());

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_executor.shutdown();

        try {
            m_executor.awaitTermination();

            LOGGER.debug("Shutdown of EventExecutor successful");
        } catch (final InterruptedException e) {
            LOGGER.warn("Could not wait for event executor thread pool to finish. Interrupted");
        }

        m_executor = null;

        return true;
    }

    /**
     * Wrapper class to execute the firing of an event i.e. the calling
     * of the listeners with the event fired as parameter in a separate thread.
     *
     * @param <T>
     *         Type of the event.
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
     */
    private static class FireEvent<T extends AbstractEvent> implements Runnable {
        private AbstractEvent m_event;
        private ArrayList<EventListener<?>> m_listener;

        /**
         * Constructor
         *
         * @param p_event
         *         Event to fire.
         * @param p_listener
         *         List of listeners to receive the event.
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
