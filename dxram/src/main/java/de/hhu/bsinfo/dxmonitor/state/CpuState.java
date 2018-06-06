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

package de.hhu.bsinfo.dxmonitor.state;

import java.util.StringTokenizer;

import de.hhu.bsinfo.dxmonitor.error.CantReadFileException;
import de.hhu.bsinfo.dxmonitor.error.InvalidCoreNumException;

/**
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class CpuState extends AbstractState {

    private int[] m_stats;
    private int m_coreNum;

    /**
     * Creates a cpu state.
     *
     * @param p_coreNum
     *     Specifies the number of the core which should be monitor
     */
    public CpuState(final int p_coreNum) {
        m_stats = new int[8];  // 0 usr - 1 nice - 2 sys - 3 idle - 4 iowait - 5 irq - 6 softirq - 7 total
        if (p_coreNum > Runtime.getRuntime().availableProcessors()) {
            try {
                throw new InvalidCoreNumException(p_coreNum);
            } catch (InvalidCoreNumException e) {
                e.printStackTrace();
            }
        }
        m_coreNum = p_coreNum;
        m_stats[7] = 0;
        updateStats();

    }

    /**
     * Reads the content of the /proc/stat file to generate a cpu state. This method will be called automatically
     * in the construtor. In addition to that this method can be used to refresh the state
     */
    @Override
    public void updateStats() {
        String cpuState = readCompleteFile("/proc/stat");
        if (cpuState == null) {
            try {
                throw new CantReadFileException("/proc/stat");
            } catch (CantReadFileException e) {
                e.printStackTrace();
            }

        }

        StringTokenizer tokenizer = new StringTokenizer(cpuState, "\n");
        for (int i = 0; i < m_coreNum; i++) {
            tokenizer.nextToken();
        }

        tokenizer = new StringTokenizer(tokenizer.nextToken(), " ");
        tokenizer.nextElement(); // skip the cpuX token

        for (int i = 0; i < 7; i++) {
            m_stats[i] = Integer.parseInt(tokenizer.nextToken());
            m_stats[7] += m_stats[i];
        }
    }

    /**
     * Returns the number of clock cycles (in Jiffies) that processes have spent in user mode on this core/cpu
     *
     * @return Usr-time measured in Jiffies
     */
    public int getUsr() {
        return m_stats[0];
    }

    /**
     * Returns the number of clock cycles that niced processes have spent in user mode on this core/cpu
     *
     * @return Nice-time measured in Jiffies
     */
    public int getNice() {
        return m_stats[1];
    }

    /**
     * Returns the number of clock cycles that processes have spent in kernel mode on this core/cpu
     *
     * @return Kernel-time measured in Jiffies
     */
    public int getSys() {
        return m_stats[2];
    }

    /**
     * Returns the number of clock cycles where the cpu/core was doing nothing
     *
     * @return Idle-time measured in Jiffies
     */
    public int getIdle() {
        return m_stats[3];
    }

    /**
     * Returns the number of clock cycles this core have spent waiting for I/O to complete
     *
     * @return IoWait-time measured in Jiffies
     */
    public int getIoWait() {
        return m_stats[4];
    }

    /**
     * Returns the number of clock cycles that this core have spent for servicing interrupts
     *
     * @return Irq-time measured in Jiffies
     */
    public int getIrq() {
        return m_stats[5];
    }

    /**
     * Returns the number of clock cycles that this core have spent for servicing software interrupts
     *
     * @return SoftIeq-time measured in Jiffies
     */
    public int getSoftIrq() {
        return m_stats[6];
    }

    /**
     * Returns the total number of clock cycles.
     *
     * @return Total number of Jiffies
     */
    public int getTotal() {
        return m_stats[7];
    }

    /**
     * Returns the total number of seconds the system has been up.
     *
     * @return time since boot
     */
    public float getUptime() {
        String tmp = readCompleteFile("/proc/uptime");
        if (tmp == null) {
            try {
                throw new CantReadFileException("/proc/uptime");
            } catch (CantReadFileException e) {
                e.printStackTrace();
            }

        }
        return Float.parseFloat(tmp.substring(0, tmp.indexOf(' ')));
    }

    /**
     * Returns the averaged system load over 1, 5 and 15 minutes.
     *
     * @return Array of averaged system load over the last minute, 5 minutes and 15 minutes
     */
    public float[] getLoadAverage() {
        String tmp = readCompleteFile("/proc/loadavg");
        if (tmp == null) {
            try {
                throw new CantReadFileException("/proc/loadavg");
            } catch (CantReadFileException e) {
                e.printStackTrace();
            }

        }
        float[] loads = new float[3];
        for (int i = 0; i < 3; i++) {
            int index = tmp.indexOf(' ');
            loads[i] = Float.parseFloat(tmp.substring(0, index));
            tmp = tmp.substring(index + 1);
        }
        return loads;
    }

    @Override
    public String toString() {
        return "{usr:" + m_stats[0] + ' ' + "nice:" + m_stats[1] + ' ' + "sys:" + m_stats[2] + ' ' + "idle:" + m_stats[3] + ' ' + "iowait:" + m_stats[4] + ' ' +
            "irq:" + m_stats[5] + ' ' + "softirq:" + m_stats[6] + '}';
    }
}
