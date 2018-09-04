package de.hhu.bsinfo.dxram.log;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Config for the LogComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = LogComponent.class, supportsSuperpeer = false, supportsPeer = true)
public class LogComponentConfig extends DXRAMComponentConfig {

    private static final int COLD_DATA_THRESHOLD = 9000;

    /**
     * Harddrive access mode ("raf" -> RandomAccessFile, "dir" -> file access with ODirect (skips kernel buffer),
     * "raw" -> direct access to raw partition).
     */
    @Expose
    private String m_harddriveAccess = "raf";

    /**
     * Path of raw device (only used for harddrive access mode "raw").
     */
    @Expose
    private String m_rawDevicePath = "/dev/raw/raw1";

    /**
     * Whether to log with checksum for every log entry or not (if true, checksum is verified during recovery).
     */
    @Expose
    private boolean m_useChecksums = true;

    /**
     * Whether to log with timestamp for every log entry or not
     * (if true, timestamps are used for improved segment selection).
     */
    @Expose
    private boolean m_useTimestamps = true;

    /**
     * The flash page size of the underlying hardware/harddrive.
     */
    @Expose
    private StorageUnit m_flashPageSize = new StorageUnit(4, StorageUnit.KB);

    /**
     * The segment size. Every secondary log is split into segments which are reorganized/recovered separately.
     */
    @Expose
    private StorageUnit m_logSegmentSize = new StorageUnit(8, StorageUnit.MB);

    /**
     * Size of the primary log. The primary log stores log entries temporary to improve SSD access.
     */
    @Expose
    private StorageUnit m_primaryLogSize = new StorageUnit(256, StorageUnit.MB);

    /**
     * The write buffer size. Every log entry is written to the write buffer first.
     */
    @Expose
    private StorageUnit m_writeBufferSize = new StorageUnit(32, StorageUnit.MB);

    /**
     * Number of bytes buffered until data is flushed to specific secondary log.
     */
    @Expose
    private StorageUnit m_secondaryLogBufferSize = new StorageUnit(128, StorageUnit.KB);

    /**
     * Threshold to activate automatic reorganization.
     **/
    @Expose
    private int m_utilizationActivateReorganization = 60;

    /**
     * Threshold to trigger low-priority reorganization (high-priority -> not enough space to write data).
     **/
    @Expose
    private int m_utilizationPromptReorganization = 75;

    /**
     * Log entries older than this threshold are not considered for segment age calculation
     * (relevant, only, if timestamps are enabled).
     **/
    @Expose
    private int m_coldDataThresholdInSec = COLD_DATA_THRESHOLD;

    public LogComponentConfig() {

    }

    public LogComponentConfig(final String p_harddriveAccess, final String p_rawDevicePath,
            final boolean p_useChecksums, final boolean p_useTimestamps, final int p_flashPageSize,
            final int p_logSegmentSize, final int p_primaryLogSize, final int p_writeBufferSize,
            final int p_secondaryLogBufferSize, final int p_utilizationActivateReorganization,
            final int p_utilizationPromptReorganization, final int p_coldDataThresholdSec) {
        m_harddriveAccess = p_harddriveAccess;
        m_rawDevicePath = p_rawDevicePath;
        m_useChecksums = p_useChecksums;
        m_useTimestamps = p_useTimestamps;
        m_flashPageSize = new StorageUnit(p_flashPageSize, StorageUnit.MB);
        m_logSegmentSize = new StorageUnit(p_logSegmentSize, StorageUnit.MB);
        m_primaryLogSize = new StorageUnit(p_primaryLogSize, StorageUnit.MB);
        m_writeBufferSize = new StorageUnit(p_writeBufferSize, StorageUnit.MB);
        m_secondaryLogBufferSize = new StorageUnit(p_secondaryLogBufferSize, StorageUnit.KB);
        m_utilizationActivateReorganization = p_utilizationActivateReorganization;
        m_utilizationPromptReorganization = p_utilizationPromptReorganization;
        m_coldDataThresholdInSec = p_coldDataThresholdSec;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {

        StorageUnit backupRangeSize = p_config.getComponentConfig(BackupComponentConfig.class).getBackupRangeSize();
        long secondaryLogSize = backupRangeSize.getBytes() * 2;

        if (m_primaryLogSize.getBytes() % m_flashPageSize.getBytes() != 0 ||
                m_primaryLogSize.getBytes() <= m_flashPageSize.getBytes() ||
                secondaryLogSize % m_flashPageSize.getBytes() != 0 || secondaryLogSize <= m_flashPageSize.getBytes() ||
                m_writeBufferSize.getBytes() % m_flashPageSize.getBytes() != 0 ||
                m_writeBufferSize.getBytes() <= m_flashPageSize.getBytes() ||
                m_logSegmentSize.getBytes() % m_flashPageSize.getBytes() != 0 ||
                m_logSegmentSize.getBytes() <= m_flashPageSize.getBytes() ||
                m_secondaryLogBufferSize.getBytes() % m_flashPageSize.getBytes() != 0 ||
                m_secondaryLogBufferSize.getBytes() <= m_flashPageSize.getBytes()) {
            LOGGER.error("Primary log size, secondary log size, write buffer size, log segment size and secondary " +
                    "log buffer size must be a multiple (integer) of and greater than flash page size");
            return false;
        }

        if (m_primaryLogSize.getBytes() % m_logSegmentSize.getBytes() != 0 ||
                m_primaryLogSize.getBytes() <= m_logSegmentSize.getBytes() ||
                secondaryLogSize % m_logSegmentSize.getBytes() != 0 ||
                secondaryLogSize <= m_logSegmentSize.getBytes() ||
                m_writeBufferSize.getBytes() % m_logSegmentSize.getBytes() != 0 ||
                m_writeBufferSize.getBytes() <= m_logSegmentSize.getBytes()) {
            LOGGER.error("Primary log size, secondary log size and write buffer size must be a multiple " +
                    "(integer) of and greater than segment size");
            return false;
        }

        if (m_secondaryLogBufferSize.getBytes() > m_logSegmentSize.getBytes()) {
            LOGGER.error("Secondary log buffer size must not exceed segment size!");
            return false;
        }

        if (m_utilizationPromptReorganization <= 50) {
            LOGGER.warn("Reorganization threshold is < 50. Reorganization is triggered continuously!");
            return true;
        }

        if (!m_useTimestamps && m_coldDataThresholdInSec != COLD_DATA_THRESHOLD) {
            LOGGER.warn("Cold data threshold was modified, but timestamps are disabled!");
        }

        return true;
    }
}
