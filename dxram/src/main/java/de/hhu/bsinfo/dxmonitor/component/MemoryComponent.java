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

import de.hhu.bsinfo.dxmonitor.state.MemState;

/**
 * Checks if the Memory is not in an invalid state by checking if the thresholds have not been exceeded. This class
 * can also be used to get an overview of the component.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class MemoryComponent extends AbstractComponent {

    private MemState m_currentState;

    private float m_freeMemPercent;
    private float m_usedMemPercent;
    private float m_cacheMemPercent;
    private float m_bufferMemPercent;
    private float m_availableMemPercent;
    private float m_freeThreshold;
    private float m_bufferThreshold;
    private float m_cacheThreshold;
    private float m_usageThreshold;
    private float m_availableThreshold;

    /**
     * This constructor should be used in callback mode. All threshold values should be entered in percent (e.g. 0.25
     * 25 %)
     *
     * @param p_usage
     *     memory usage threshold
     * @param p_free
     *     free threshold
     * @param p_available
     *     available memory threshold
     * @param p_buffer
     *     filebuffer threshold
     * @param p_cache
     *     cache threshold
     */
    public MemoryComponent(final float p_usage, final float p_free, final float p_available, final float p_buffer, final float p_cache,
        final MonitorCallbackInterface p_callbackInterface) {

        m_usageThreshold = p_usage;
        m_bufferThreshold = p_buffer;
        m_freeThreshold = p_free;
        m_availableMemPercent = p_available;
        m_cacheThreshold = p_cache;
        m_callback = true;
        m_callbackIntf = p_callbackInterface;

        m_currentState = new MemState();
    }

    /**
     * This constructor should be used in overview mode.
     */
    public MemoryComponent(final float p_secondDelay) {
        m_callback = false;
        m_secondDelay = p_secondDelay;

        m_currentState = new MemState();
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void analyse() {
        StringBuilder callBack = new StringBuilder("Memory:\n");
        if (m_bufferThreshold - m_bufferMemPercent < 0) {
            callBack.append("Buffer Callback\n");
        }
        if (m_usageThreshold - m_usedMemPercent < 0) {
            callBack.append("Usage Callback\n");
        }
        if (m_freeThreshold - m_freeMemPercent > 0) {
            callBack.append("Free Callback\n");
        }
        if (m_availableThreshold - m_availableMemPercent > 0) {
            callBack.append("Available Callback\n");
        }
        if (m_cacheThreshold - m_cacheMemPercent < 0) {
            callBack.append("Cache Callback\n");
        }

        String tmp = callBack.toString();
        //if(tmp.equals("Memory:\n")) output = tmp+"no callbacks!\n";
        //else output = tmp;
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void updateData() {
        m_currentState.updateStats();
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void calculate() {
        m_freeMemPercent =
            ((float) m_currentState.getFreeMem() + m_currentState.getCacheSize() + m_currentState.getBufferSize()) / m_currentState.getTotalMem();
        m_usedMemPercent = (float) m_currentState.getUsedMem() / m_currentState.getTotalMem();
        m_cacheMemPercent = (float) m_currentState.getCacheSize() / m_currentState.getTotalMem();
        m_bufferMemPercent = (float) m_currentState.getBufferSize() / m_currentState.getTotalMem();
        m_availableMemPercent = (float) m_currentState.getAvailableMem() / m_currentState.getTotalMem();
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public String toString() {
        return String.format("======== Memory ========\n" + "Free: %2.2f\tAvailable: %2.2f\tUsage: %2.2f\tCache: %2.2f\tBuffer: %2.2f\n\n", m_freeMemPercent,
            m_availableMemPercent, m_usedMemPercent, m_cacheMemPercent, m_bufferMemPercent);
    }

    public float getMemoryUsage() {
        return m_usedMemPercent;
    }
}
