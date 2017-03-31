/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxcompute;

import de.hhu.bsinfo.dxcompute.job.JobService;
import de.hhu.bsinfo.dxcompute.job.JobWorkStealingComponent;
import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;

/**
 * Main class/entry point for any application to work with DXCompute and its services.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.10.2016
 */
public class DXCompute extends DXRAM {

    /**
     * Constructor
     */
    public DXCompute() {
        super();
    }

    @Override
    protected void registerComponents(final DXRAMEngine p_engine) {
        super.registerComponents(p_engine);

        p_engine.registerComponent(JobWorkStealingComponent.class);
    }

    @Override
    protected void registerServices(final DXRAMEngine p_engine) {
        super.registerServices(p_engine);

        p_engine.registerService(JobService.class);
        p_engine.registerService(MasterSlaveComputeService.class);
    }
}
