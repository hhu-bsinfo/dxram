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

/**
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class MemState extends AbstractState {

    private long[] m_stats;

    /**
     * Creates a memory state by calling the updateStats().
     */
    public MemState() {
        m_stats = new long[5];

        updateStats();
    }

    /**
     * Reads the content of the /proc/meminfo file to generate a memory state. This method will be called automatically
     * in the construtor. In addition to that this method can be used to refresh the state.
     */
    @Override
    public void updateStats() {
        String fileContent = readCompleteFile("/proc/meminfo");
        if (fileContent == null) {
            try {
                throw new CantReadFileException("/proc/meminfo");
            } catch (CantReadFileException e) {
                e.printStackTrace();
            }

        }
        StringTokenizer tokenizer = new StringTokenizer(fileContent, "\n");
        for (int i = 0; i < 5; i++) {
            String tmp = tokenizer.nextToken();

            String t = tmp.substring(0, tmp.lastIndexOf(' '));
            m_stats[i] = Long.parseLong(t.substring(t.lastIndexOf(' ') + 1));
        }
    }

    /**
     * Returns the total RAM size (measured in kb).
     *
     * @return total ram size (measured in kb)
     */
    public long getTotalMem() {
        return m_stats[0];
    }

    /**
     * Returns the space of the memory that is free (measured in kb).
     *
     * @return free ram space (measured in kb)
     */
    public long getFreeMem() {
        return m_stats[1];
    }

    /**
     * Returns the space of the ram that is currently in use (measured in kb).
     *
     * @return ram space in use (measured in kb)
     */
    public long getUsedMem() {
        return m_stats[0] - m_stats[1] - m_stats[3] - m_stats[4];
    }

    /**
     * Returns the number of kilobytes that is available for an application without swapping.
     *
     * @return number of available kb
     */
    public long getAvailableMem() {
        return m_stats[2];
    }

    /**
     * Return the number of size that is used for cache.
     *
     * @return cache size in ram
     */
    public long getCacheSize() {
        return m_stats[4];
    }

    /**
     * Returns the number of size that is used as a file buffer
     *
     * @return file buffer size in ram
     */
    public long getBufferSize() {
        return m_stats[3];
    }

    public String toString() {
        return "Mem: {" + "total: " + m_stats[0] + "KB, " + "free: " + m_stats[1] + "KB, " + "used: " + (m_stats[0] - m_stats[1]) + "KB, " + "available: " +
            m_stats[2] + "KB, " + "cache: " + m_stats[4] + "KB, " + "buffer: " + m_stats[3] + "KB" + '}';
    }

}
