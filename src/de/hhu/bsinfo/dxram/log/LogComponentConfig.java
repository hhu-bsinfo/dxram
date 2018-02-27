package de.hhu.bsinfo.dxram.log;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponentConfig;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Config for the LogComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class LogComponentConfig extends AbstractDXRAMComponentConfig {

    private static final int COLD_DATA_THRESHOLD = 9000;

    @Expose
    private String m_harddriveAccess = "raf";

    @Expose
    private String m_rawDevicePath = "/dev/raw/raw1";

    @Expose
    private boolean m_useChecksums = true;

    @Expose
    private boolean m_useTimestamps = true;

    @Expose
    private StorageUnit m_flashPageSize = new StorageUnit(4, StorageUnit.KB);

    @Expose
    private StorageUnit m_logSegmentSize = new StorageUnit(8, StorageUnit.MB);

    @Expose
    private StorageUnit m_primaryLogSize = new StorageUnit(256, StorageUnit.MB);

    @Expose
    private StorageUnit m_writeBufferSize = new StorageUnit(256, StorageUnit.MB);

    @Expose
    private StorageUnit m_secondaryLogBufferSize = new StorageUnit(128, StorageUnit.KB);

    @Expose
    private int m_reorgUtilizationThreshold = 70;

    @Expose
    private int m_coldDataThresholdInSec = COLD_DATA_THRESHOLD;

    /**
     * Constructor
     */
    public LogComponentConfig() {
        super(LogComponent.class, false, true);
    }

    public LogComponentConfig(final String p_harddriveAccess, final String p_rawDevicePath, final boolean p_useChecksums, final boolean p_useTimestamps,
            final int p_flashPageSize, final int p_logSegmentSize, final int p_primaryLogSize, final int p_writeBufferSize, final int p_secondaryLogBufferSize,
            final int p_reorgUtilizationThreshold, final int p_coldDataThresholdSec) {
        super(LogComponent.class, false, true);

        m_harddriveAccess = p_harddriveAccess;
        m_rawDevicePath = p_rawDevicePath;
        m_useChecksums = p_useChecksums;
        m_useTimestamps = p_useTimestamps;
        m_flashPageSize = new StorageUnit(p_flashPageSize, StorageUnit.MB);
        m_logSegmentSize = new StorageUnit(p_logSegmentSize, StorageUnit.MB);
        m_primaryLogSize = new StorageUnit(p_primaryLogSize, StorageUnit.MB);
        m_writeBufferSize = new StorageUnit(p_writeBufferSize, StorageUnit.MB);
        m_secondaryLogBufferSize = new StorageUnit(p_secondaryLogBufferSize, StorageUnit.KB);
        m_reorgUtilizationThreshold = p_reorgUtilizationThreshold;
        m_coldDataThresholdInSec = p_coldDataThresholdSec;
    }

    /**
     * Harddrive access mode ("raf" -> RandomAccessFile, "dir" -> file access with ODirect (skips kernel buffer), "raw" -> direct access to raw partition).
     */
    public String getHarddriveAccess() {
        return m_harddriveAccess;
    }

    /**
     * Path of raw device (only used for harddrive access mode "raw").
     */
    public String getRawDevicePath() {
        return m_rawDevicePath;
    }

    /**
     * Whether to log with checksum for every log entry or not (if true, checksum is verified during recovery).
     */
    public boolean useChecksums() {
        return m_useChecksums;
    }

    /**
     * Whether to log with timestamp for every log entry or not (if true, timestamps are used for improved segment selection).
     */
    public boolean useTimestamps() {
        return m_useTimestamps;
    }

    /**
     * The flash page size of the underlying hardware/harddrive.
     */
    public StorageUnit getFlashPageSize() {
        return m_flashPageSize;
    }

    /**
     * The segment size. Every secondary log is split into segments which are reorganized/recovered separately.
     */
    public StorageUnit getLogSegmentSize() {
        return m_logSegmentSize;
    }

    /**
     * Size of the primary log. The primary log stores log entries temporary to improve SSD access.
     */
    public StorageUnit getPrimaryLogSize() {
        return m_primaryLogSize;
    }

    /**
     * The write buffer size. Every log entry is written to the write buffer first.
     */
    public StorageUnit getWriteBufferSize() {
        return m_writeBufferSize;
    }

    /**
     * Number of bytes buffered until data is flushed to specific secondary log.
     */
    public StorageUnit getSecondaryLogBufferSize() {
        return m_secondaryLogBufferSize;
    }

    /**
     * Threshold to trigger low-priority reorganization (high-priority -> not enough space to write data).
     **/
    public int getReorgUtilizationThreshold() {
        return m_reorgUtilizationThreshold;
    }

    /**
     * Log entries older than this threshold are not considered for segment age calculation (relevant, only, if timestamps are enabled).
     **/
    public int getColdDataThreshold() {
        return m_coldDataThresholdInSec;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {

        StorageUnit backupRangeSize = p_config.getComponentConfig(BackupComponentConfig.class).getBackupRangeSize();
        long secondaryLogSize = backupRangeSize.getBytes() * 2;

        if (m_primaryLogSize.getBytes() % m_flashPageSize.getBytes() != 0 || m_primaryLogSize.getBytes() <= m_flashPageSize.getBytes() ||
                secondaryLogSize % m_flashPageSize.getBytes() != 0 || secondaryLogSize <= m_flashPageSize.getBytes() ||
                m_writeBufferSize.getBytes() % m_flashPageSize.getBytes() != 0 || m_writeBufferSize.getBytes() <= m_flashPageSize.getBytes() ||
                m_logSegmentSize.getBytes() % m_flashPageSize.getBytes() != 0 || m_logSegmentSize.getBytes() <= m_flashPageSize.getBytes() ||
                m_secondaryLogBufferSize.getBytes() % m_flashPageSize.getBytes() != 0 || m_secondaryLogBufferSize.getBytes() <= m_flashPageSize.getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Primary log size, secondary log size, write buffer size, log segment size and secondary log buffer size " +
                    "must be a multiple (integer) of and greater than flash page size");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_primaryLogSize.getBytes() % m_logSegmentSize.getBytes() != 0 || m_primaryLogSize.getBytes() <= m_logSegmentSize.getBytes() ||
                secondaryLogSize % m_logSegmentSize.getBytes() != 0 || secondaryLogSize <= m_logSegmentSize.getBytes() ||
                m_writeBufferSize.getBytes() % m_logSegmentSize.getBytes() != 0 || m_writeBufferSize.getBytes() <= m_logSegmentSize.getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Primary log size, secondary log size and write buffer size must be a multiple (integer) of and greater than segment size");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_secondaryLogBufferSize.getBytes() > m_logSegmentSize.getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Secondary log buffer size must not exceed segment size!");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_reorgUtilizationThreshold <= 50) {
            // #if LOGGER >= WARN
            LOGGER.warn("Reorganization threshold is < 50. Reorganization is triggered continuously!");
            // #endif /* LOGGER >= WARN */
            return true;
        }

        if (!m_useTimestamps && m_coldDataThresholdInSec != COLD_DATA_THRESHOLD) {
            // #if LOGGER >= WARN
            LOGGER.warn("Cold data threshold was modified, but timestamps are disabled!");
            // #endif /* LOGGER >= WARN */
        }

        if (secondaryLogSize < p_config.getComponentConfig(MemoryManagerComponentConfig.class).getKeyValueStoreMaxBlockSize().getBytes() ||
                m_writeBufferSize.getBytes() < p_config.getComponentConfig(MemoryManagerComponentConfig.class).getKeyValueStoreMaxBlockSize().getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Secondary log and write buffer size must be greater than the max size of a chunk");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }
}
