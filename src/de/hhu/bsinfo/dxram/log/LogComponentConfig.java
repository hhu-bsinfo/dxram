package de.hhu.bsinfo.dxram.log;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * Config for the LogComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class LogComponentConfig extends DXRAMComponentConfig {
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
    private StorageUnit m_secondaryLogSize = new StorageUnit(512, StorageUnit.MB);

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

    // TODO kevin: doc
    public String getHarddriveAccess() {
        return m_harddriveAccess;
    }

    // TODO kevin: doc
    public String getRawDevicePath() {
        return m_rawDevicePath;
    }

    // TODO kevin: doc
    public boolean useChecksum() {
        return m_useChecksum;
    }

    // TODO kevin: doc
    public StorageUnit getFlashPageSize() {
        return m_flashPageSize;
    }

    // TODO kevin: doc
    public StorageUnit getLogSegmentSize() {
        return m_logSegmentSize;
    }

    // TODO kevin: doc
    public StorageUnit getPrimaryLogSize() {
        return m_primaryLogSize;
    }

    // TODO kevin: doc
    public StorageUnit getSecondaryLogSize() {
        return m_secondaryLogSize;
    }

    // TODO kevin: doc
    public StorageUnit getWriteBufferSize() {
        return m_writeBufferSize;
    }

    // TODO kevin: doc
    public StorageUnit getSecondaryLogBufferSize() {
        return m_secondaryLogSize;
    }

    // TODO kevin: doc
    public int getReorgUtilizationThreshold() {
        return m_reorgUtilizationThreshold;
    }

    // TODO kevin: doc
    public boolean sortBufferPooling() {
        return m_sortBufferPooling;
    }
}
