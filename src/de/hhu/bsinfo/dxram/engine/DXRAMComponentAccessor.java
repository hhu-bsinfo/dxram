/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.engine;

/**
 * Interface to access loaded components of the engine
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public interface DXRAMComponentAccessor {

    /**
     * Get a component from the engine.
     *
     * @param <T>
     *     Type of the component class.
     * @param p_class
     *     Class of the component to get. If the component has different implementations, use the common
     *     interface
     *     or abstract class to get the registered instance.
     * @return Reference to the component if available and enabled, null otherwise or if the engine is not
     * initialized.
     */
    <T extends AbstractDXRAMComponent> T getComponent(Class<T> p_class);
}
