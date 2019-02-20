/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.chunk.operation;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 19.02.2019
 *
 */
public class RawReadLocal extends AbstractOperation {

    private static final ThroughputPool SOP_RAW_READ_BYTES =
            new ThroughputPool(ChunkLocalService.class, "RawReadBytes", Value.Base.B_10);

    static {
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_RAW_READ_BYTES);
    }

    /**
     * Constructor.
     *
     * @param p_parentService
     *         Instance of parent service this operation belongs to
     * @param p_boot
     *         Instance of BootComponent
     * @param p_backup
     *         Instance of BackupComponent
     * @param p_chunk
     *         Instance of ChunkComponent
     * @param p_network
     *         Instance of NetworkComponent
     * @param p_lookup
     *         Instance of LookupComponent
     * @param p_nameservice
     *         Instance of NameserviceComponent
     */
    public RawReadLocal(
            final Class<? extends AbstractDXRAMService<? extends DXRAMModuleConfig>> p_parentService,
            final AbstractBootComponent<?> p_boot,
            final BackupComponent p_backup,
            final ChunkComponent p_chunk,
            final NetworkComponent p_network,
            final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);
    }

    /**
     * Read a single byte from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @return Byte read.
     */
    public byte readByte(final long p_address, final int p_addressOffset) {
        SOP_RAW_READ_BYTES.start(1);
        final byte tmpByte = m_chunk.getMemory().rawRead().readByte(p_address, p_addressOffset);
        SOP_RAW_READ_BYTES.stop();
        return tmpByte;
    }

    /**
     * Read a single char from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @return Char read.
     */
    public char readChar(final long p_address, final int p_addressOffset) {
        SOP_RAW_READ_BYTES.start(1);
        final char tmpChar = m_chunk.getMemory().rawRead().readChar(p_address, p_addressOffset);
        SOP_RAW_READ_BYTES.stop();
        return tmpChar;
    }

    /**
     * Read a single int from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @return Int read.
     */
    public int readInt(final long p_address, final int p_addressOffset) {
        SOP_RAW_READ_BYTES.start(1);
        final int tmpInt = m_chunk.getMemory().rawRead().readInt(p_address, p_addressOffset);
        SOP_RAW_READ_BYTES.stop();
        return tmpInt;
    }

    /**
     * Read a long from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @return Long read.
     */
    public long readLong(final long p_address, final int p_addressOffset) {
        SOP_RAW_READ_BYTES.start(1);
        final long tmpLong = m_chunk.getMemory().rawRead().readLong(p_address, p_addressOffset);
        SOP_RAW_READ_BYTES.stop();
        return tmpLong;
    }

    /**
     * Read a single short from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @return Short read.
     */
    public short readShort(final long p_address, final int p_addressOffset) {
        SOP_RAW_READ_BYTES.start(1);
        final short tmpShort = m_chunk.getMemory().rawRead().readShort(p_address, p_addressOffset);
        SOP_RAW_READ_BYTES.stop();
        return tmpShort;
    }

    /**
     * Read data into a byte array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to read into.
     */
    public void readByteArray(final long p_address, final int p_addressOffset, final byte[] p_array) {
        SOP_RAW_READ_BYTES.start(p_array.length);
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, p_array);
        SOP_RAW_READ_BYTES.stop();
    }

    /**
     * Read data into a new byte array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_count
     *         Number of bytes to read.
     * @return A new byte array with the read data.
     */
    public byte[] readByteArray(final long p_address, final int p_addressOffset, final int p_count) {
        SOP_RAW_READ_BYTES.start(p_count);
        final byte[] tmpArray = new byte[p_count];
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, tmpArray);
        SOP_RAW_READ_BYTES.stop();
        return tmpArray;
    }

    /**
     * Read data into a char array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to read into.
     */
    public void readCharArray(final long p_address, final int p_addressOffset, final char[] p_array) {
        SOP_RAW_READ_BYTES.start(p_array.length * Character.BYTES);
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, p_array);
        SOP_RAW_READ_BYTES.stop();
    }

    /**
     * Read data into a new char array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_count
     *         Number of chars to read.
     * @return A new char array with the read data.
     */
    public char[] readCharArray(final long p_address, final int p_addressOffset, final int p_count) {
        SOP_RAW_READ_BYTES.start(p_count * Character.BYTES);
        final char[] tmpArray = new char[p_count];
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, tmpArray);
        SOP_RAW_READ_BYTES.stop();
        return tmpArray;
    }

    /**
     * Read data into a int array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to read into.
     */
    public void readIntArray(final long p_address, final int p_addressOffset, final int[] p_array) {
        SOP_RAW_READ_BYTES.start(p_array.length * Integer.BYTES);
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, p_array);
        SOP_RAW_READ_BYTES.stop();
    }

    /**
     * Read data into a new integer array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_count
     *         Number of integers to read.
     * @return A new integer array with the read data.
     */
    public int[] readIntArray(final long p_address, final int p_addressOffset, final int p_count) {
        SOP_RAW_READ_BYTES.start(p_count * Integer.BYTES);
        final int[] tmpArray = new int[p_count];
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, tmpArray);
        SOP_RAW_READ_BYTES.stop();
        return tmpArray;
    }

    /**
     * Read data into a long array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to read into.
     */
    public void readLongArray(final long p_address, final int p_addressOffset, final long[] p_array) {
        SOP_RAW_READ_BYTES.start(p_array.length * Long.BYTES);
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, p_array);
        SOP_RAW_READ_BYTES.stop();
    }

    /**
     * Read data into a new long array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_count
     *         Number of longs to read.
     * @return A new long array with the read data.
     */
    public long[] readLongArray(final long p_address, final int p_addressOffset, final int p_count) {
        SOP_RAW_READ_BYTES.start(p_count * Long.BYTES);
        final long[] tmpArray = new long[p_count];
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, tmpArray);
        SOP_RAW_READ_BYTES.stop();
        return tmpArray;
    }

    /**
     * Read data into a short array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to read into.
     */
    public void readShortArray(final long p_address, final int p_addressOffset, final short[] p_array) {
        SOP_RAW_READ_BYTES.start(p_array.length * Short.BYTES);
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, p_array);
        SOP_RAW_READ_BYTES.stop();
    }

    /**
     * Read data into a new short array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_count
     *         Number of shorts to read.
     * @return A new short array with the read data.
     */
    public short[] readShortArray(final long p_address, final int p_addressOffset, final int p_count) {
        SOP_RAW_READ_BYTES.start(p_count * Short.BYTES);
        final short[] tmpArray = new short[p_count];
        m_chunk.getMemory().rawRead().read(p_address, p_addressOffset, tmpArray);
        SOP_RAW_READ_BYTES.stop();
        return tmpArray;
    }
}
