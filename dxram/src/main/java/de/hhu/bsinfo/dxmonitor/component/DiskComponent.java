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

import de.hhu.bsinfo.dxmonitor.state.DiskState;

/**
 * Checks if the Disk is not in an invalid state by checking if the thresholds have not been exceeded. This class
 * can also be used to get an overview of the component.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class DiskComponent extends AbstractComponent {

    private String m_name;
    private float m_readPercent = 0;
    private float m_writePercent = 0;
    private float m_readSpeed = 0;
    private float m_writeSpeed = 0;
    private DiskState m_currentStat;
    private DiskState m_lastStat;
    private float m_readThreshold;
    private float m_writeThreshold;

    /**
     * This constructor should be used in callback mode. All threshold values should be entered in percent (e.g. 0.25
     * 25 %)
     *
     * @param p_name
     *     name of the disk
     * @param p_read
     *     percentage disposition of the read operations
     * @param p_write
     *     percentage disposition of the write operations
     * @param p_secondDelay
     *     refresh rate
     */
    public DiskComponent(final String p_name, final float p_read, final float p_write, final float p_secondDelay,
        final MonitorCallbackInterface p_monitorCallbackInterface) {

        m_callback = true;
        m_name = p_name;
        m_readThreshold = p_read;
        m_writeThreshold = p_write;
        m_secondDelay = p_secondDelay;
        m_callbackIntf = p_monitorCallbackInterface;

        m_currentStat = new DiskState(m_name);
        m_startTime = System.currentTimeMillis();
        m_endTime = System.currentTimeMillis();
    }

    /**
     * This constructor should be used in overview mode.
     *
     * @param p_name
     *     name of the disk
     * @param p_secondDelay
     *     refresh rate
     */
    public DiskComponent(final String p_name, final float p_secondDelay) {
        m_callback = false;
        m_name = p_name;
        m_secondDelay = p_secondDelay;

        m_currentStat = new DiskState(m_name);
        m_startTime = System.currentTimeMillis();
        m_endTime = System.currentTimeMillis();
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void analyse() {
        /*StringBuilder callBack = new StringBuilder("Disk " + m_name + ":\n");

        if (m_readThreshold + 1 > 0) {
            if (m_readThreshold - m_readPercent - 0.05 < 0) {
                callBack.append("Too less reads - Read Callback\n");
            } else if (m_readThreshold - m_readPercent + 0.05 > 0) {
                callBack.append("Too many reads - Read Callback\n");
            }
        }

        if (m_writeThreshold + 1 > 0) {
            if (m_writeThreshold - m_writePercent - 0.05 < 0) {
                callBack.append("Too less writes - Write Callback\n");
            } else if (m_writeThreshold - m_writePercent + 0.05 > 0) {
                callBack.append("Too many writes - Write Callback\n");
            }
        }

        // String tmp = callBack.toString();
        //if(tmp.equals("Disk "+name+":\n")) output = tmp+"no callbacks!\n";
        //else output = tmp;
        */
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void updateData() {
        m_lastStat = m_currentStat;
        m_currentStat = new DiskState(m_name);
        m_endTime = System.currentTimeMillis();
        m_secondDelay = (m_endTime - m_startTime) / 1000.0f;
        m_startTime = m_endTime;

    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void calculate() {
        m_readPercent = 0;
        m_readSpeed = 0;
        m_writePercent = 0;
        m_writeSpeed = 0;
        long totalOperations = m_currentStat.getRead() - m_lastStat.getRead() + m_currentStat.getWrite() - m_lastStat.getWrite();
        if (m_currentStat.getRead() - m_lastStat.getRead() != 0) {
            m_readPercent = ((float) m_currentStat.getRead() - m_lastStat.getRead()) / totalOperations;
            m_readSpeed = ((float) m_currentStat.getReadBytes() - m_lastStat.getReadBytes()) / m_secondDelay / 1000.0f; // KB/s
        }
        if (m_currentStat.getWrite() - m_lastStat.getWrite() != 0) {
            m_writePercent = ((float) m_currentStat.getWrite() - m_lastStat.getWrite()) / totalOperations;
            m_writeSpeed = ((float) m_currentStat.getWriteBytes() - m_lastStat.getWriteBytes()) / m_secondDelay / 1000.0f; // KB/s
        }
    }


    /**
     * @see AbstractComponent
     */
    @Override
    public String toString() {
        return String.format("======== Disk ========\n" + "Write: %2.2f\tRead: %2.2f\tWrite: %10.2f kb/s\tRead: %10.2f kb/s\n\n", m_writePercent, m_readPercent,
            m_writeSpeed, m_readSpeed);
    }

    public float getReadPercent() {
        return m_readPercent;
    }

    public float getWritePercent() {
        return m_writePercent;
    }
}
