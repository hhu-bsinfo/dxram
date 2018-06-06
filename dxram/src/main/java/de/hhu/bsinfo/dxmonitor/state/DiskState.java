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

import de.hhu.bsinfo.dxmonitor.error.CantReadFileException;
import de.hhu.bsinfo.dxmonitor.error.NoValidDiskException;

/**
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class DiskState extends AbstractState {

    private String m_name;

    private long m_size;
    private long m_sectorSize;
    private long m_read;
    private long m_readSectors;
    private long m_write;
    private long m_writeSectors;

    /**
     * Creates an state of the given disk. In addition to that the functions to initialize the sector size
     * and the total size of the disk will be called.
     *
     * @param p_name
     *     Name of the disk (e.g. sda)
     */
    public DiskState(final String p_name) {
        m_name = p_name;

        updateStats();
        setDiskSizeFromFile();
        setSectorSizeFromFile();
    }

    /**
     * Reads the content of the /proc/diskstat file to generate a disk state. This method will be called automatically
     * in the construtor. In addition to that this method can be used to refresh the state.
     */
    @Override
    public void updateStats() {
        String tmp = readLine(m_name);
        if (tmp == null) {
            try {
                throw new CantReadFileException("/proc/diskstats");
            } catch (CantReadFileException e) {
                e.printStackTrace();
            }

        }
        String[] fileInput = tmp.substring(tmp.indexOf(m_name)).split(" ");
        m_read = Long.parseLong(fileInput[1]);
        m_readSectors = Long.parseLong(fileInput[3]);
        m_write = Long.parseLong(fileInput[5]);
        m_writeSectors = Long.parseLong(fileInput[7]);
    }

    /**
     * Return the size of the disk.
     *
     * @return size of disk
     */
    public long getSize() {
        return m_size;
    }

    /**
     * Returns the number of successful read-operations on this disk.
     *
     * @return number of read ops
     */
    public long getRead() {
        return m_read;
    }

    /**
     * Returns the number of  read sector from this disk.
     *
     * @return number of read sector
     */
    public long getReadSectors() {
        return m_readSectors;
    }

    /**
     * Returns the number of bytes that have been read from this disk
     *
     * @return number of read bytes
     */
    public long getReadBytes() {
        return m_readSectors * m_sectorSize;
    }

    /**
     * Returns the number of successful write-operations on this disk.
     *
     * @return number of write ops
     */
    public long getWrite() {
        return m_write;
    }

    /**
     * Returns the number of write sector from this disk.
     *
     * @return number of write sector
     */
    public long getWriteSectors() {
        return m_writeSectors;
    }

    /**
     * Returns the number of bytes that have been written from this disk
     *
     * @return number of written bytes
     */
    public long getWriteBytes() {
        return m_writeSectors * m_sectorSize;
    }

    public String toString() {
        return "Disk: {" + "size: " + m_size + "read: " + m_read + "readSectors: " + m_readSectors + "write: " + m_write + "writtenSectors: " + m_writeSectors +
            '}';
    }

    // Hilfsfunkionen

    /**
     * Search for the given disk name in the specified file. If the disk name is represented in this file the line will
     * be returned as a String.
     *
     * @param p_name
     *     Name of the disk
     * @return line of the specified file
     */
    private static String readLine(final String p_name) {
        String output = null;
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("/proc/diskstats"));
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                if (tmp.contains(p_name)) {
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

    /**
     * Reads the total disk size from the /sys/block/DISKNAME/size file.
     */
    private void setDiskSizeFromFile() {
        String tmp = readCompleteFile("/sys/block/" + m_name + "/size");
        if (tmp == null) {
            int index = 0;
            while (Character.isAlphabetic(m_name.charAt(index))) {
                index++;
            }
            tmp = readCompleteFile("/sys/block/" + m_name.substring(0, index) + '/' + m_name + "/size");
            if (tmp == null) {
                try {
                    throw new NoValidDiskException(m_name);
                } catch (NoValidDiskException e) {
                    e.printStackTrace();
                }
            }
        }

        int index = 0;
        while (Character.isDigit(tmp.charAt(index++))) {
        }

        m_size = Long.parseLong(tmp.substring(0, index - 1));
    }

    /**
     * Reads the sector size from the /sys/block/DISKNAME/queue/hw_sector_size file.
     */
    private void setSectorSizeFromFile() {
        String tmp = readCompleteFile("/sys/block/" + m_name + "/queue/hw_sector_size");
        if (tmp == null) {
            int index = 0;
            while (Character.isAlphabetic(m_name.charAt(index))) {
                index++;
            }
            tmp = readCompleteFile("/sys/block/" + m_name.substring(0, index) + "/queue/hw_sector_size");
            if (tmp == null) {
                try {
                    throw new NoValidDiskException(m_name);
                } catch (NoValidDiskException e) {
                    e.printStackTrace();
                }
            }
        }

        int index = 0;
        while (Character.isDigit(tmp.charAt(index++))) {
        }
        m_sectorSize = Long.parseLong(tmp.substring(0, index - 1));
    }

}
