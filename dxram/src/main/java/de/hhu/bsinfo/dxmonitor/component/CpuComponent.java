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

import de.hhu.bsinfo.dxmonitor.error.InvalidCoreNumException;
import de.hhu.bsinfo.dxmonitor.state.CpuState;

/**
 * Checks if the Cpu is not in an invalid state by checking if the thresholds have not been exceeded. This class
 * can also be used to get an overview of the component.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class CpuComponent extends AbstractComponent {

    private CpuState m_lastState;
    private CpuState m_currentState;

    private int m_coreNum;

    private float m_cpuUsage;
    private float m_idleUsage;
    private float m_usrUsage;
    private float m_sysUsage;
    private float m_niceUsage;
    private float m_softIrqUsage;
    private float m_irqUsage;
    private float m_ioWaitUsage;
    private float[] m_loads;
    private float m_load1Threshold;
    private float m_load5Threshold;
    private float m_usageThreshold;

    /**
     * This constructor should be used for the callback mode. All threshold values should be entered in percent (e.g. 0.25
     * 25 %)
     *
     * @param p_coreNum
     *     number of the core
     * @param p_usage
     *     threshold for cpu usage
     * @param p_load1
     *     threshold for the average system load over the last minute
     * @param p_load5
     *     threshold for the average system load over the last five minutes
     */
    public CpuComponent(final int p_coreNum, final float p_usage, final float p_load1, final float p_load5,
        final MonitorCallbackInterface p_monitorCallbackInterface) {

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        m_callback = true;
        m_callbackIntf = p_monitorCallbackInterface;

        if (p_coreNum <= availableProcessors) {
            m_currentState = new CpuState(p_coreNum);
        } else {
            try {
                throw new InvalidCoreNumException(p_coreNum);
            } catch (InvalidCoreNumException e) {
                e.printStackTrace();
            }
        }

        m_coreNum = p_coreNum;
        m_usageThreshold = p_usage;
        m_load1Threshold = p_load1 * availableProcessors;
        m_load5Threshold = p_load5 * availableProcessors;
    }

    /**
     * This consturctor should be used for the overview mode.
     *
     * @param p_coreNum
     *     number of the core
     */
    public CpuComponent(final int p_coreNum, final float p_delay) {
        m_callback = false;
        if (p_coreNum <= Runtime.getRuntime().availableProcessors()) {
            m_currentState = new CpuState(p_coreNum);
        } else {
            try {
                throw new InvalidCoreNumException(p_coreNum);
            } catch (InvalidCoreNumException e) {
                e.printStackTrace();
            }
        }
        m_secondDelay = p_delay;
        m_coreNum = p_coreNum;
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void updateData() {
        m_lastState = m_currentState;
        m_currentState = new CpuState(m_coreNum);
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void analyse() {
        m_callbackIntf.handle();
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void calculate() {
        if (m_lastState != null) {
            float totalDiff = m_currentState.getTotal() - m_lastState.getTotal();
            m_cpuUsage = 1.0f - ((float) m_currentState.getIdle() - m_lastState.getIdle()) / totalDiff;
            m_idleUsage = ((float) m_currentState.getIdle() - m_lastState.getIdle()) / totalDiff;
            m_sysUsage = ((float) m_currentState.getSys() - m_lastState.getSys()) / totalDiff;
            m_usrUsage = ((float) m_currentState.getUsr() - m_lastState.getUsr()) / totalDiff;
            m_niceUsage = ((float) m_currentState.getNice() - m_lastState.getNice()) / totalDiff;
            m_softIrqUsage = ((float) m_currentState.getSoftIrq() - m_lastState.getSoftIrq()) / totalDiff;
            m_irqUsage = ((float) m_currentState.getIrq() - m_lastState.getIrq()) / totalDiff;
            m_ioWaitUsage = ((float) m_currentState.getIoWait() - m_lastState.getIoWait()) / totalDiff;
        } else {
            m_cpuUsage = 1.0f - (float) m_currentState.getIdle() / m_currentState.getTotal();
            m_idleUsage = (float) m_currentState.getIdle() / m_currentState.getTotal();
            m_sysUsage = (float) m_currentState.getSys() / m_currentState.getTotal();
            m_usrUsage = (float) m_currentState.getUsr() / m_currentState.getTotal();
            m_niceUsage = (float) m_currentState.getNice() / m_currentState.getTotal();
            m_softIrqUsage = (float) m_currentState.getSoftIrq() / m_currentState.getTotal();
            m_irqUsage = (float) m_currentState.getIrq() / m_currentState.getTotal();
        }
        m_loads = m_currentState.getLoadAverage();
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public String toString() {
        return String
            .format("======== CPU ========\n" + "Usage: %2.2f \tusr-time: %2.2f \tsys-time: %2.2f \tidle-time: %2.2f\n\n", m_cpuUsage, m_usrUsage, m_sysUsage,
                m_idleUsage);
    }

    public float getCpuUsage() {
        return m_cpuUsage;
    }

    public float[] getCpuLoads() {
        return m_loads;
    }

}
