/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating
 * Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage.writebuffer;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.MessageHeader;
import de.hhu.bsinfo.dxnet.core.MessageImporterDefault;
import de.hhu.bsinfo.dxram.log.storage.Scheduler;
import de.hhu.bsinfo.dxram.log.storage.logs.LogHandler;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.Version;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionHandler;
import de.hhu.bsinfo.dxutils.ByteBufferHelper;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;

/**
 * For accessing the write buffer from outside of this package.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public final class WriteBufferHandler {

    private static final TimePool SOP_LOG_BATCH = new TimePool(WriteBufferHandler.class, "LogBatch");
    private static final TimePool SOP_PUT_ENTRY_AND_HEADER = new TimePool(WriteBufferHandler.class, "LogSingle");

    static {
        StatisticsManager.get().registerOperation(WriteBufferHandler.class, SOP_LOG_BATCH);
        StatisticsManager.get().registerOperation(WriteBufferHandler.class, SOP_PUT_ENTRY_AND_HEADER);
    }

    private final VersionHandler m_versionHandler;

    private final WriteBuffer m_writeBuffer;
    private final ProcessThread m_processThread;

    private final boolean m_useTimestamps;
    private final long m_initTime;

    /**
     * Creates an instance of WriteBufferHandler.
     * Creates and initializes all write buffer components.
     *
     * @param p_logHandler
     *         the log handler needed by the processing thread for posting write jobs
     * @param p_versionHandler
     *         the version handler for getting the next version for a chunk
     * @param p_scheduler
     *         the scheduler needed by the processing thread to grant access to the reorganization thread
     * @param p_bufferPool
     *         the buffer pool storing buffer wrappers
     * @param p_writeBufferSize
     *         the write buffer size
     * @param p_secondaryLogBufferSize
     *         the secondary log buffer size
     * @param p_flashPageSize
     *         the flash page size
     * @param p_useChecksum
     *         whether checksums are used or not
     * @param p_useTimestamps
     *         whether timestamps are used or not
     * @param p_initTime
     *         the initialization time or not if timestamps are disabled
     */
    public WriteBufferHandler(final LogHandler p_logHandler, final VersionHandler p_versionHandler,
            final Scheduler p_scheduler, final BufferPool p_bufferPool, final int p_writeBufferSize,
            final int p_secondaryLogBufferSize, final int p_flashPageSize, final boolean p_useChecksum,
            final boolean p_useTimestamps, final long p_initTime) {
        m_versionHandler = p_versionHandler;

        m_useTimestamps = p_useTimestamps;
        m_initTime = p_initTime;

        m_writeBuffer =
                new WriteBuffer(p_writeBufferSize, p_flashPageSize, p_useChecksum, p_logHandler.getWriteCapacity());

        m_processThread = new ProcessThread(p_logHandler, p_scheduler, m_writeBuffer, p_bufferPool,
                (int) (p_writeBufferSize * 0.45), p_secondaryLogBufferSize);
        m_processThread.setName("Logging: Process Thread");
        m_processThread.start();
    }

    /**
     * Closes all write buffer components.
     */
    public void close() {
        m_processThread.close();
        m_writeBuffer.close();
    }

    /**
     * Flush the write buffer.
     */
    public void flushWriteBuffer() {
        m_writeBuffer.flush();
    }

    /**
     * Flush the write buffer. Waits for a specific range to be flushed.
     *
     * @param p_owner
     *         the owner
     * @param p_range
     *         the range ID
     */
    public void flushWriteBuffer(final short p_owner, final short p_range) {
        m_writeBuffer.flush(p_owner, p_range);
    }

    /**
     * Posts data on write buffer. Called after message serialization (if message was split).
     *
     * @param p_owner
     *         the owner
     * @param p_rangeID
     *         the range ID
     * @param p_numberOfDataStructures
     *         the number of data structures to write
     * @param p_buffer
     *         the buffer containing all data structures
     */
    public void postData(final short p_owner, final short p_rangeID, final int p_numberOfDataStructures,
            final ByteBuffer p_buffer) {
        MessageImporterDefault importer = new MessageImporterDefault();
        importer.setBuffer(ByteBufferHelper.getDirectAddress(p_buffer), p_buffer.capacity(), 0);
        importer.setNumberOfReadBytes(0);

        processDataStructures(importer, p_numberOfDataStructures, p_owner, p_rangeID);
    }

    /**
     * Posts data on write buffer. Includes serialization (if message is complete)
     *
     * @param p_messageHeader
     *         the message header containing all information to access the messages's data
     */
    public void postData(final MessageHeader p_messageHeader) {
        MessageImporterDefault importer = new MessageImporterDefault();
        p_messageHeader.initExternalImporter(importer);

        short owner = p_messageHeader.getSource();
        short rangeID = importer.readShort((short) 0);
        int numberOfDataStructures = importer.readInt(0);

        processDataStructures(importer, numberOfDataStructures, owner, rangeID);

    }

    /**
     * Iterates the data structures to be written to write buffer.
     *
     * @param p_importer
     *         the message importer
     * @param p_numberOfDataStructures
     *         the number of data structures
     * @param p_owner
     *         the owner
     * @param p_rangeID
     *         the range ID
     */
    private void processDataStructures(final MessageImporterDefault p_importer, final int p_numberOfDataStructures,
            final short p_owner, final short p_rangeID) {
        Version version;
        long chunkID = ChunkID.INVALID_ID;
        int length = -1;

        SOP_LOG_BATCH.start();

        int timestamp = -1;
        if (m_useTimestamps) {
            // Getting the same timestamp for all chunks to be logged
            // This might be a little inaccurate but currentTimeMillis is expensive
            timestamp = (int) ((System.currentTimeMillis() - m_initTime) / 1000);
        }

        for (int i = 0; i < p_numberOfDataStructures; i++) {
            chunkID = p_importer.readLong(chunkID);
            length = p_importer.readCompactNumber(length);

            assert length > 0;

            SOP_PUT_ENTRY_AND_HEADER.start();

            version = m_versionHandler.getVersion(chunkID, p_owner, p_rangeID);

            m_writeBuffer.putLogData(p_importer, chunkID, length, p_rangeID, p_owner, version, timestamp);

            SOP_PUT_ENTRY_AND_HEADER.stop();
        }

        SOP_LOG_BATCH.stop();
    }
}
