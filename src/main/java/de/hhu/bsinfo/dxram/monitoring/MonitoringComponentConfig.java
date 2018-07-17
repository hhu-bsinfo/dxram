/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.monitoring;

import java.io.File;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxmonitor.util.DeviceLister;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxutils.OSValidator;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Monitoring Component config.
 *
 * @author Burak Akguel, burak.akguel@hhu.de 14.07.2018
 */
public class MonitoringComponentConfig extends AbstractDXRAMComponentConfig {
    @Expose
    private boolean m_monitoringActive = false;

    @Expose
    private String m_nic = "";

    @Expose
    private String m_disk = "";

    @Expose
    private TimeUnit m_timeWindow = new TimeUnit(2, "sec");

    @Expose
    private short m_collectsPerWindow = 10;

    @Expose
    private String m_monitoringFolder = "./mon";

    private TimeUnit m_csvTimeWindow;

    public MonitoringComponentConfig() {
        super(MonitoringComponent.class, true, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (!OSValidator.isUnix()) {
            LOGGER.error("Monitoring is only supported for unix operating systems.");
            return false;
        }

        if (!m_nic.isEmpty() && !DeviceLister.getNICs().contains(m_nic)) {
            LOGGER.error("Monitoring component - m_nic [%s] is invalid", m_nic);
            return false;
        }

        if (!m_disk.isEmpty() && !DeviceLister.getDisks().contains(m_disk)) {
            LOGGER.error("Monitoring component - m_diskIdentifier [%s] is invalid", m_disk);
            return false;
        }

        if (m_monitoringFolder.isEmpty()) {
            m_monitoringFolder = System.getProperty("user.dir") + "/mon";
        }
        
        File file = new File(m_monitoringFolder);

        if (!file.exists()) {
            if (!file.mkdirs()) {
                LOGGER.error("Monitoring folder [%s] seems to be invalid - didn't exist and couldn't be created!", m_monitoringFolder);
                return false;
            }
        }

        LOGGER.debug("Monitoring data output folder: %s", file);

        // after 8 "windows" the data will be written to file
        m_csvTimeWindow = new TimeUnit(m_timeWindow.getSec() * 8, "sec");

        return true;
    }

    /**
     * Returns true if monitoring is active.
     */
    public boolean isMonitoringActive() {
        return m_monitoringActive;
    }

    /**
     * Returns NIC identifier.
     */
    public String getNic() {
        return m_nic;
    }

    /**
     * Returns disk identifier.
     */
    public String getDisk() {
        return m_disk;
    }

    /**
     * Returns path to monitoring folder.
     */
    public String getMonitoringFolder() {
        return m_monitoringFolder;
    }

    /**
     * Returns the amount seconds that builds a time window.
     */
    public float getSecondsTimeWindow() {
        return m_timeWindow.getSec();
    }

    /**
     * Returns the amount of collects in a single time window
     */
    public short getCollectsPerWindow() {
        return m_collectsPerWindow;
    }

    /**
     * Returns the amount of seconds after which the csv files are written.
     */
    public float getCSVSecondsTimeWindow() {
        return m_csvTimeWindow.getSec();
    }
}
