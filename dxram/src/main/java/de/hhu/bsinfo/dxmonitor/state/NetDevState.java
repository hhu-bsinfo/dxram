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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import de.hhu.bsinfo.dxmonitor.error.CantReadFileException;
import de.hhu.bsinfo.dxmonitor.error.UnsupportedInterfaceException;

/**
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class NetDevState extends AbstractState {

    private String m_name;
    private long m_speed;
    private long[] m_receiveStats;
    private long[] m_transmitStats;

    /**
     * Creates a state of a given nic.
     *
     * @param p_name
     *     name of the nic
     */
    public NetDevState(final String p_name) {
        if (p_name.length() < 1) {
            m_name = "lo";
        } else {
            m_name = p_name;
        }
        m_receiveStats = new long[4]; // bytes - packets - errs - drop
        m_transmitStats = new long[5]; // bytes - packets - errs - drop - collisions

        try {
            setSpeedFromFile();
            updateStats();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Reads the maximum speed in MB/s of the nic from the /sys/class/net/NIC/speed file.
     */
    private void setSpeedFromFile() throws UnsupportedInterfaceException {
        if (m_name.charAt(0) == 'e') {
            String speedString = readCompleteFile("/sys/class/net/" + m_name + "/speed");
            if (speedString != null) {
                int index = 0;
                while (Character.isDigit(speedString.charAt(index))) {
                    index++;
                }
                m_speed = 1024 * 1024 * Long.parseLong(speedString.substring(0, index));
            } else {
                try {
                    throw new CantReadFileException("/sys/class/net/" + m_name + "/speed");
                } catch (CantReadFileException e) {
                    e.printStackTrace();
                }
            }
        } else if (m_name.charAt(0) == 'w') {
            // wireless interface
            throw new UnsupportedInterfaceException(m_name);
        } else if (m_name.charAt(0) == 'i') {
            //infiniband interface
            throw new UnsupportedInterfaceException(m_name);
        } else {
            throw new UnsupportedInterfaceException(m_name);
        }
    }

    /**
     * Reads the contect of the /proc/net/dev file to initialize the variables or to refresh them.
     */
    @Override
    public void updateStats() {
        String stats = readLine(m_name);

        if (stats == null) {
            // throw exception
            try {
                throw new CantReadFileException("/proc/net/dev");
            } catch (CantReadFileException e) {
                e.printStackTrace();
            }
        } else {
            StringTokenizer tokenizer = new StringTokenizer(stats, " ");
            tokenizer.nextToken();
            for (int i = 0; i < 8; i++) {
                if (i < 4) {
                    m_receiveStats[i] = Long.parseLong(tokenizer.nextToken());
                } else {
                    tokenizer.nextToken();
                }
            }
            for (int i = 0; i < 5; i++) {
                m_transmitStats[i] = Long.parseLong(tokenizer.nextToken());
            }
        }

    }

    /**
     * Returns the maximum possible speed of the nic.
     *
     * @return max. speed of the nic
     */
    public long getSpeed() {
        return m_speed;
    }

    /**
     * Returns the number of received bytes.
     *
     * @return received bytes
     */
    public long getReceiveBytes() {
        return m_receiveStats[0];
    }

    /**
     * Returns the number of received packets.
     *
     * @return number of received packets
     */
    public long getReceivePackets() {
        return m_receiveStats[1];
    }

    /**
     * Returns the number of received faulty packets.
     *
     * @return number of faulty packets
     */
    public long getReceiveErrors() {
        return m_receiveStats[2];
    }

    /**
     * Returns the number of dropped packets.
     *
     * @return number of dropped packets
     */
    public long getReceiveDrops() {
        return m_receiveStats[3];
    }

    /**
     * Returns the number of transmitted bytes.
     *
     * @return transmitted bytes
     */
    public long getTransmitBytes() {
        return m_transmitStats[0];
    }

    /**
     * Returns the number of transmitted packets.
     *
     * @return number of transmitted packets
     */
    public long getTransmitPackets() {
        return m_transmitStats[1];
    }

    /**
     * Returns the number of transmitted faulty packets.
     *
     * @return number of faulty packets
     */
    public long getTransmitErrors() {
        return m_transmitStats[2];
    }

    /**
     * Returns the number of transmitted dropped packets.
     *
     * @return number of dropped packets
     */
    public long getTransmitDrops() {
        return m_transmitStats[3];
    }

    @Override
    public String toString() {
        return Arrays.toString(m_receiveStats) + Arrays.toString(m_transmitStats) + ' ' + m_speed;
    }

    // Hilfsmethoden

    /**
     * Search for the given disk name in the specified file. If the disk name is represented in this file the line will
     * be returned as a String.
     *
     * @param p_name
     *     Name of the nic
     * @return line of the specified file
     */
    private String readLine(final String p_name) {
        String output = null;
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("/proc/net/dev"));
            reader.readLine();
            reader.readLine();
            String tmp = null;
            while ((tmp = reader.readLine()) != null) {
                if (tmp.substring(0, tmp.indexOf(':')).contains(p_name)) {
                    output = tmp;
                    break;
                }
            }
            reader.close();
        } catch (IOException e) {
            //e.printStackTrace(); // File not exists
            return null;
        }
        return output;
    }
}
