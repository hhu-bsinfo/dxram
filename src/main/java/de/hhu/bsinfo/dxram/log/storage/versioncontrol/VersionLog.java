package de.hhu.bsinfo.dxram.log.storage.versioncontrol;

import java.io.File;
import java.io.IOException;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.logs.Log;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * For writing versions to disk.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 13.09.2018
 */
final class VersionLog extends Log {

    private static final TimePool SOP_READ_VERSIONS_LOG_TIME = new TimePool(VersionLog.class, "ReadVersionsLogTime");
    private static final TimePool SOP_WRITE_VERSIONS_LOG_TIME = new TimePool(VersionLog.class, "WriteVersionsLogTime");
    private static final ValuePool SOP_WRITE_VERSIONS_LOG_SIZE =
            new ValuePool(VersionLog.class, "WriteVersionsLogSize");

    static {
        StatisticsManager.get().registerOperation(VersionLog.class, SOP_READ_VERSIONS_LOG_TIME);
        StatisticsManager.get().registerOperation(VersionLog.class, SOP_WRITE_VERSIONS_LOG_TIME);
        StatisticsManager.get().registerOperation(VersionLog.class, SOP_WRITE_VERSIONS_LOG_SIZE);
    }

    /**
     * Initializes a versions log.
     *
     * @param p_logFile
     *         the file
     */
    VersionLog(final File p_logFile) throws IOException {
        super(p_logFile, 0);

        createLog();
    }

    /**
     * Update file name after recovery
     *
     * @param p_newFile
     *         the new file name
     */
    void transferBackupRange(final String p_newFile) throws IOException {
        renameLog(new File(p_newFile));
    }

    @Override
    public final void readFromLog(final DirectByteBufferWrapper p_bufferWrapper, final int p_length,
            final long p_readPos) throws IOException {

        SOP_READ_VERSIONS_LOG_TIME.start();

        final long bytesUntilEnd = p_length - p_readPos;

        if (p_length > 0) {
            assert p_length <= bytesUntilEnd;

            ms_logAccess.read(m_log, p_bufferWrapper, p_length, p_readPos);
        }

        SOP_READ_VERSIONS_LOG_TIME.stop();
    }

    @Override
    public final void writeToLog(DirectByteBufferWrapper p_data, final int p_bufferOffset, final long p_writePos,
            int p_length, final boolean p_accessed) throws IOException {

        SOP_WRITE_VERSIONS_LOG_SIZE.add(p_length);
        SOP_WRITE_VERSIONS_LOG_TIME.start();

        ms_logAccess.write(m_log, p_data, p_bufferOffset, p_writePos, p_length, true);

        SOP_WRITE_VERSIONS_LOG_TIME.stop();
    }

    @Override
    public final long appendToLog(DirectByteBufferWrapper p_data, final int p_bufferOffset, int p_length)
            throws IOException {

        SOP_WRITE_VERSIONS_LOG_SIZE.add(p_length);
        SOP_WRITE_VERSIONS_LOG_TIME.start();

        long fileSize = getFileSize();
        ms_logAccess.append(m_log, p_data, p_bufferOffset, getFileSize(), p_length);

        SOP_WRITE_VERSIONS_LOG_TIME.stop();

        return fileSize + p_length;
    }
}