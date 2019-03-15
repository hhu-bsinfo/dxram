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
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
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
public class RawWriteLocal extends Operation {

    private static final ThroughputPool SOP_RAW_WRITE_BYTES =
            new ThroughputPool(ChunkLocalService.class, "RawWriteBytes", Value.Base.B_10);

    static {
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_RAW_WRITE_BYTES);
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
    public RawWriteLocal(
            final Class<? extends Service<? extends ModuleConfig>> p_parentService,
            final BootComponent<?> p_boot,
            final BackupComponent p_backup,
            final ChunkComponent p_chunk,
            final NetworkComponent p_network,
            final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);
    }

    /**
     * Write a single boolean to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @param p_value
     *         Boolean value to write.
     */
    public void writeBoolean(final long p_address, final int p_addressOffset, final boolean p_value) {
        SOP_RAW_WRITE_BYTES.start(Byte.BYTES);
        m_chunk.getMemory().rawWrite().writeBoolean(p_address, p_addressOffset, p_value);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write a single byte to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @param p_value
     *         Byte value to write.
     */
    public void writeByte(final long p_address, final int p_addressOffset, final byte p_value) {
        SOP_RAW_WRITE_BYTES.start(Byte.BYTES);
        m_chunk.getMemory().rawWrite().writeByte(p_address, p_addressOffset, p_value);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write a single char to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @param p_value
     *         Char value to write.
     */
    public void writeChar(final long p_address, final int p_addressOffset, final char p_value) {
        SOP_RAW_WRITE_BYTES.start(Character.BYTES);
        m_chunk.getMemory().rawWrite().writeChar(p_address, p_addressOffset, p_value);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write a single int to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @param p_value
     *         Int value to write.
     */
    public void writeInt(final long p_address, final int p_addressOffset, final int p_value) {
        SOP_RAW_WRITE_BYTES.start(Integer.BYTES);
        m_chunk.getMemory().rawWrite().writeInt(p_address, p_addressOffset, p_value);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write a single long to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @param p_value
     *         Long value to write.
     */
    public void writeLong(final long p_address, final int p_addressOffset, final long p_value) {
        SOP_RAW_WRITE_BYTES.start(Long.BYTES);
        m_chunk.getMemory().rawWrite().writeLong(p_address, p_addressOffset, p_value);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write a single short to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @param p_value
     *         Short value to write.
     */
    public void writeShort(final long p_address, final int p_addressOffset, final short p_value) {
        SOP_RAW_WRITE_BYTES.start(Short.BYTES);
        m_chunk.getMemory().rawWrite().writeShort(p_address, p_addressOffset, p_value);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write a single double to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @param p_value
     *         Double value to write.
     */
    public void writeDouble(final long p_address, final int p_addressOffset, final double p_value) {
        SOP_RAW_WRITE_BYTES.start(Double.BYTES);
        m_chunk.getMemory().rawWrite().writeDouble(p_address, p_addressOffset, p_value);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write a single float to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to the address.
     * @param p_value
     *         Float value to write.
     */
    public void writeFloat(final long p_address, final int p_addressOffset, final float p_value) {
        SOP_RAW_WRITE_BYTES.start(Float.BYTES);
        m_chunk.getMemory().rawWrite().writeFloat(p_address, p_addressOffset, p_value);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write data from a boolean array to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to write.
     */
    public void writeBooleanArray(final long p_address, final int p_addressOffset, final boolean[] p_array) {
        SOP_RAW_WRITE_BYTES.start(p_array.length);
        m_chunk.getMemory().rawWrite().write(p_address, p_addressOffset, p_array);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write data from a byte array to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to write.
     */
    public void writeByteArray(final long p_address, final int p_addressOffset, final byte[] p_array) {
        SOP_RAW_WRITE_BYTES.start(p_array.length);
        m_chunk.getMemory().rawWrite().write(p_address, p_addressOffset, p_array);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write data from a char array to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to write.
     */
    public void writeCharArray(final long p_address, final int p_addressOffset, final char[] p_array) {
        SOP_RAW_WRITE_BYTES.start(p_array.length * Character.BYTES);
        m_chunk.getMemory().rawWrite().write(p_address, p_addressOffset, p_array);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write data from an int array to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to write.
     */
    public void writeIntArray(final long p_address, final int p_addressOffset, final int[] p_array) {
        SOP_RAW_WRITE_BYTES.start(p_array.length * Integer.BYTES);
        m_chunk.getMemory().rawWrite().write(p_address, p_addressOffset, p_array);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write data from a long array to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to write.
     */
    public void writeLongArray(final long p_address, final int p_addressOffset, final long[] p_array) {
        SOP_RAW_WRITE_BYTES.start(p_array.length * Long.BYTES);
        m_chunk.getMemory().rawWrite().write(p_address, p_addressOffset, p_array);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write data from a short array to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to write.
     */
    public void writeShortArray(final long p_address, final int p_addressOffset, final short[] p_array) {
        SOP_RAW_WRITE_BYTES.start(p_array.length * Short.BYTES);
        m_chunk.getMemory().rawWrite().write(p_address, p_addressOffset, p_array);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write data from a double array to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to write.
     */
    public void writeDoubleArray(final long p_address, final int p_addressOffset, final double[] p_array) {
        SOP_RAW_WRITE_BYTES.start(p_array.length * Double.BYTES);
        m_chunk.getMemory().rawWrite().write(p_address, p_addressOffset, p_array);
        SOP_RAW_WRITE_BYTES.stop();
    }

    /**
     * Write data from a float array to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_addressOffset
     *         Offset to add to start address.
     * @param p_array
     *         Array to write.
     */
    public void writeFloatArray(final long p_address, final int p_addressOffset, final float[] p_array) {
        SOP_RAW_WRITE_BYTES.start(p_array.length * Float.BYTES);
        m_chunk.getMemory().rawWrite().write(p_address, p_addressOffset, p_array);
        SOP_RAW_WRITE_BYTES.stop();
    }
}
