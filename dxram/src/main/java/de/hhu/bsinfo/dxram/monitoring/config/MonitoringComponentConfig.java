package de.hhu.bsinfo.dxram.monitoring.config;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxmonitor.util.DeviceLister;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.monitoring.MonitoringComponent;
import de.hhu.bsinfo.dxutils.OSValidator;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

import java.io.File;

public class MonitoringComponentConfig extends AbstractDXRAMComponentConfig {

    @Expose
    private String m_nic = "enp4s0f0";

    @Expose
    private String m_disk = "sda1";

    @Expose
    private TimeUnit m_timeWindow = new TimeUnit(2, "sec");

    @Expose
    private short m_collectsPerWindow = 10;

    @Expose
    private String m_monitoringFolder = System.getProperty("user.home") + "/dxmon";

    private TimeUnit m_csvTimeWindow;



    public MonitoringComponentConfig() {
        super(MonitoringComponent.class, true, true);
    }

    @Override
    protected boolean verify(DXRAMContext.Config p_config) {
        if(!OSValidator.isUnix()) {
            LOGGER.error("Monitoring is only supported for unix operating systems. Fix your configuration");
            return false;
        }

        if(!DeviceLister.getNICs().contains(m_nic)) {
            LOGGER.error("Monitoring component - m_nic ["+ m_nic +"] is invalid");
            return false;
        }

        if(!DeviceLister.getDisks().contains(m_disk)) {
            LOGGER.error("Monitoring component - m_diskIdentifier ["+ m_disk +"] is invalid");
            return false;
        }

        File file = new File(m_monitoringFolder);
        if(!file.exists()) {
            if(!file.mkdirs()) {
                LOGGER.error("Monitoring folder ["+ m_monitoringFolder +"] seems to be invalid - didn't exist and couldn't be created!");
                return false;
            }
        }

        // after 8 "windows" the data will be written to file
        m_csvTimeWindow = new TimeUnit(m_timeWindow.getSec() * 8, "sec");

        return true;
    }

    public String getNic() {
        return m_nic;
    }

    public String getDisk() {
        return m_disk;
    }

    public String getMonitoringFolder() {
        return m_monitoringFolder;
    }

    public float getSecondsTimeWindow() {
        return m_timeWindow.getSec();
    }

    public short getCollectsPerWindow() {
        return m_collectsPerWindow;
    }

    public float getCSVSecondsTimeWindow() {
        return m_csvTimeWindow.getSec();
    }

}
