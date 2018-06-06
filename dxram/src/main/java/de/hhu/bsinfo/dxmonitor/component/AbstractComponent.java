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

package de.hhu.bsinfo.dxmonitor.component;


/**
 * This class is a template and should be used for own implementations of components to be able to use the scheduler.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public abstract class AbstractComponent {

    /**
     * Variable that will be printed in each round.
     */
    //protected String output;
    float m_secondDelay;
    long m_startTime;
    long m_endTime;

    MonitorCallbackInterface m_callbackIntf;

    boolean m_callback;

    AbstractComponent() {
    }

    /**
     * This method checks if the handler class has exceeded given thresholds.
     */
    public abstract void analyse();

    /**
     * Updates the internal state data structures.
     */
    public abstract void updateData();

    /**
     * This method calculates with the help of state classes other important information about the component.
     */
    public abstract void calculate();

    /**
     * will be used later to switch callbacks
     *
     * @param p_callbackIntf
     */
    public void setCallback(final MonitorCallbackInterface p_callbackIntf) {
        m_callbackIntf = p_callbackIntf;
    }
}
