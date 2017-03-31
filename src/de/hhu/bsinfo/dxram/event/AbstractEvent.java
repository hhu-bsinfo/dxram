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

/**
 * Base class for events that can be fired within DXRAM.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public abstract class AbstractEvent {
    private String m_sourceClass;

    /**
     * Constructor
     *
     * @param p_sourceClass
     *     Source class this event originates from.
     */
    protected AbstractEvent(final String p_sourceClass) {
        m_sourceClass = p_sourceClass;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '[' + m_sourceClass + ']';
    }
}
