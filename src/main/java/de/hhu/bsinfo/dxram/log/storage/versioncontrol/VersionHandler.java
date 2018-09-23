package de.hhu.bsinfo.dxram.log.storage.versioncontrol;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.storage.BackupRangeCatalog;
import de.hhu.bsinfo.dxram.log.storage.Scheduler;

/**
 * For accessing the version buffer (and log) from outside of this package.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public final class VersionHandler {

    private static final Logger LOGGER = LogManager.getFormatterLogger(VersionHandler.class.getSimpleName());

    private final Scheduler m_scheduler;
    private final BackupRangeCatalog m_backupRangeCatalog;

    private final long m_backupRangeSize;

    /**
     * Creates an instance of VersionHandler.
     *
     * @param p_scheduler
     *         the scheduler used by version buffer to trigger process thread when version buffer is full
     * @param p_backupRangeCatalog
     *         the backup range catalog to get the version buffer for a owner-rangeID pair
     * @param p_secondaryLogSize
     *         the secondary log size
     */
    public VersionHandler(final Scheduler p_scheduler, final BackupRangeCatalog p_backupRangeCatalog,
            final long p_secondaryLogSize) {
        m_scheduler = p_scheduler;
        m_backupRangeCatalog = p_backupRangeCatalog;

        m_backupRangeSize = p_secondaryLogSize / 2;
    }

    /**
     * Closes all version components.
     */
    public void close() {
        // The version buffers and logs are closed by the backup range catalog
    }

    /**
     * Creates a new version buffer (including log).
     *
     * @param p_owner
     *         the owner
     * @param p_fileName
     *         the file name including backup directory
     * @return the new version buffer
     */
    public VersionBuffer createVersionBuffer(final short p_owner, final String p_fileName) {
        return new VersionBuffer(m_scheduler, p_owner, m_backupRangeSize, p_fileName);
    }

    /**
     * Returns a new version for given chunk.
     *
     * @param p_chunkID
     *         the chunk ID
     * @param p_ownerID
     *         the owner
     * @param p_rangeID
     *         the range ID
     * @return the new version.
     */
    public Version getVersion(final long p_chunkID, final short p_ownerID, final short p_rangeID) {
        VersionBuffer versionBuffer = m_backupRangeCatalog.getVersionBuffer(p_ownerID, p_rangeID);

        if (versionBuffer == null) {
            LOGGER.error("No version buffer available for chunk 0x%d from range %d of peer 0x%d", p_chunkID, p_rangeID,
                    p_ownerID);
            return null;
        }

        return versionBuffer.getNextVersion(p_chunkID);
    }

    /**
     * Invalidates a chunk (makes all log entries of given chunk disposable).
     *
     * @param p_chunkIDs
     *         the ChunkIDs
     */
    public final void invalidateChunks(final long[] p_chunkIDs, final short p_ownerID, final short p_rangeID) {
        VersionBuffer versionBuffer = m_backupRangeCatalog.getVersionBuffer(p_ownerID, p_rangeID);

        if (versionBuffer == null) {
            LOGGER.error("No version buffer available for range %d of peer 0x%d", p_rangeID, p_ownerID);
            return;
        }

        for (int i = 0; i < p_chunkIDs.length; i++) {
            versionBuffer.putVersion(p_chunkIDs[i], Version.INVALID_VERSION);
        }
    }

    /**
     * Returns the corresponding version buffer.
     *
     * @param p_ownerID
     *         the owner
     * @param p_rangeID
     *         the range ID
     * @return the corresponding version buffer.
     */
    public VersionBuffer getVersionBuffer(final short p_ownerID, final short p_rangeID) {
        VersionBuffer versionBuffer = m_backupRangeCatalog.getVersionBuffer(p_ownerID, p_rangeID);

        if (versionBuffer == null) {
            LOGGER.error("No version buffer available for range %d of peer 0x%d", p_rangeID, p_ownerID);
        }
        return versionBuffer;
    }

    /**
     * Gets current versions from buffer and log.
     *
     * @param p_allVersions
     *         an array and hash table (for migrations) to store version numbers in
     * @return the lowest CID at the time the versions are read-in
     * @throws IOException
     *         if versions could not be read from log
     */
    public final long getCurrentVersions(final short p_ownerID, final short p_rangeID,
            final TemporaryVersionsStorage p_allVersions, final boolean p_writeBack) throws IOException {
        VersionBuffer versionBuffer = m_backupRangeCatalog.getVersionBuffer(p_ownerID, p_rangeID);

        if (versionBuffer == null) {
            LOGGER.error("No version buffer available for range %d of peer 0x%d", p_rangeID, p_ownerID);
            return -1;
        }

        if (p_writeBack) {
            // Read versions from SSD and write back current view
            return versionBuffer.readAll(p_allVersions, true);
        } else {
            return versionBuffer.readAll(p_allVersions, false);
        }
    }
}
