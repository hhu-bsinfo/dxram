package de.hhu.bsinfo.dxram.log;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * Config for the LogComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class LogComponentConfig extends AbstractDXRAMComponentConfig {
    @Expose
    private String m_harddriveAccess = "raf";

    @Expose
    private String m_rawDevicePath = "/dev/raw/raw1";

    @Expose
    private boolean m_useChecksum = true;

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
    private boolean m_sortBufferPooling = true;

    /**
     * Constructor
     */
    public LogComponentConfig() {
        super(LogComponent.class, false, true);
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
    public boolean useChecksum() {
        return m_useChecksum;
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
     * Whether to sort data of write buffer by backup range before flushing. Might improve performance.
     */
    public boolean sortBufferPooling() {
        return m_sortBufferPooling;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {

        StorageUnit backupRangeSize = p_config.getComponentConfig(BackupComponentConfig.class).getBackupRangeSize();
        long secondaryLogSize = backupRangeSize.getBytes();

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

        return true;
    }
}
