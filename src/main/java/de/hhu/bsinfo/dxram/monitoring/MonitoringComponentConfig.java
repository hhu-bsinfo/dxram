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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.IOException;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxmonitor.util.DeviceLister;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxutils.OSValidator;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Monitoring Component config.
 *
 * @author Burak Akguel, burak.akguel@hhu.de 14.07.2018
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class MonitoringComponentConfig extends ModuleConfig {
    /**
     * Returns true if monitoring is active.
     */
    @Expose
    private boolean m_monitoringActive = false;

    /**
     * Returns NIC identifier.
     */
    @Expose
    private String m_nic = "";

    /**
     * Returns disk identifier.
     */
    @Expose
    private String m_disk = "";

    /**
     * Returns the amount seconds that builds a time window.
     */
    @Expose
    private TimeUnit m_timeWindow = new TimeUnit(2, "sec");

    /**
     * Returns the amount of collects in a single time window
     */
    @Expose
    private short m_collectsPerWindow = 10;

    /**
     * Returns path to monitoring folder.
     */
    @Expose
    private String m_monitoringFolder = "./mon";

    /**
     * Returns the amount of seconds after which the csv files are written.
     */
    @Expose
    private TimeUnit m_csvTimeWindow = new TimeUnit(m_timeWindow.getSec() * 8, "sec");

    /**
     * Constructor
     */
    public MonitoringComponentConfig() {
        super(MonitoringComponent.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        /*if (!OSValidator.isUnix()) {
            LOGGER.error("Monitoring is only supported for unix operating systems.");
            return false;
        }*/

        try {
            if (!m_nic.isEmpty() && !DeviceLister.getNICs().contains(m_nic)) {
                LOGGER.error("Monitoring component - m_nic [%s] is invalid", m_nic);
                return false;
            }
        } catch (final IOException ignored) {
            LOGGER.warn("Getting available NICs to verify configured NIC failed, ignore");
        }

        try {
            if (!m_disk.isEmpty() && !DeviceLister.getDisks().contains(m_disk)) {
                LOGGER.error("Monitoring component - m_diskIdentifier [%s] is invalid", m_disk);
                return false;
            }
        } catch (final IOException ignored) {
            LOGGER.warn("Getting available disks to verify configured disk failed, ignore");
        }

        if (m_monitoringFolder.isEmpty()) {
            LOGGER.error("Monitoring folder must be set");
            return false;
        }

        return true;
    }
}
