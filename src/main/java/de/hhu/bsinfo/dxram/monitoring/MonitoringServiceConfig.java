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

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxutils.OSValidator;

/**
 * Monitoring Service config class.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class MonitoringServiceConfig extends AbstractDXRAMServiceConfig {

    /**
     * Constructor
     */
    public MonitoringServiceConfig() {
        super(MonitoringService.class, true, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (!OSValidator.isUnix()) {
            LOGGER.error("Monitoring is only supported for unix operating systems. Fix your configuration");
            return false;
        }

        return true;
    }

}
