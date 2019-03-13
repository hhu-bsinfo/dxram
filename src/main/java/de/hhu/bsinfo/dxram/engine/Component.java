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

package de.hhu.bsinfo.dxram.engine;

import java.lang.annotation.Annotation;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Base class for all components in DXRAM. A component serves the engine as a building block
 * providing features for a specific task. Splitting features and concepts
 * across multiple components allows creating a clear structure and higher flexibility
 * for the whole system. Components are allowed to depend on other components i.e. directly
 * interact with each other.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public abstract class Component<T> extends Module<T> {
    private Attributes m_attributes;

    /**
     * Constructor
     */
    protected Component() {
        super();

        Annotation[] annotations = getClass().getAnnotations();

        for (Annotation annotation : annotations) {
            if (annotation instanceof Attributes) {
                m_attributes = (Attributes) annotation;
                break;
            }
        }

        if (m_attributes == null) {
            throw new IllegalStateException("No Attributes annotation on component: " +
                    this.getClass().getSimpleName());
        }
    }

    @Override
    protected boolean moduleInit(final DXRAMEngine p_engine) {
        boolean ret;

        resolveComponentDependencies(p_engine);

        try {
            ret = initComponent(p_engine.getConfig(), p_engine.getJNIManager());
        } catch (final Exception e) {
            LOGGER.error("Initializing component failed", e);

            return false;
        }

        return ret;
    }

    @Override
    protected boolean moduleShutdown() {
        return shutdownComponent();
    }

    /**
     * Called before the component is initialized. Get all the components your own component depends on.
     *
     * @param p_componentAccessor
     *         Component accessor that provides access to other components
     */
    protected void resolveComponentDependencies(final ComponentProvider p_componentAccessor) {
    }

    /**
     * Called when the component is initialized. Setup data structures, read settings etc.
     *
     * @param p_config
     *         Configuration instance provided by the engine.
     * @param p_jniManager
     *         Instance of JNI manager to load JNI libraries
     * @return True if initialing was successful, false otherwise.
     */
    protected abstract boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager);

    /**
     * Called when the component gets shut down. Cleanup any resources of your component in here.
     *
     * @return True if shutdown was successful, false otherwise.
     */
    protected abstract boolean shutdownComponent();

    /**
     * Get the init priority.
     *
     * @return Init priority.
     */
    int getPriorityInit() {
        return m_attributes.priorityInit();
    }

    /**
     * Get the shutdown priority.
     *
     * @return Shutdown priority.
     */
    int getPriorityShutdown() {
        return m_attributes.priorityShutdown();
    }

    /**
     * Attributes for components
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    public @interface Attributes {
        /**
         * The init priority to determine initialization order
         */
        short priorityInit();

        /**
         * The shutdown priority to determine shutdown order
         */
        short priorityShutdown();
    }
}
