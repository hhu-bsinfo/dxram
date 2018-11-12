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

package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.operation.Dump;
import de.hhu.bsinfo.dxram.chunk.operation.Reset;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Special service with operations for debugging/benchmarking
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2017
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class ChunkDebugService extends AbstractDXRAMService<DXRAMModuleConfig> {
    // component dependencies
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkComponent m_chunk;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;
    private NameserviceComponent m_nameservice;

    private Dump m_dump;
    private Reset m_reset;


    /**
     * Get the dump operation
     *
     * @return Operation
     */
    public Dump dump() {
        return m_dump;
    }

    /**
     * Get the reset operation
     *
     * @return Operation
     */
    public Reset reset() {
        return m_reset;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_nameservice = p_componentAccessor.getComponent(NameserviceComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        m_dump = new Dump(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_reset = new Reset(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
