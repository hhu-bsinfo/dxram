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
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.operation.GetAnon;
import de.hhu.bsinfo.dxram.chunk.operation.PutAnon;
import de.hhu.bsinfo.dxram.engine.Inject;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Special chunk service to work with anonymous chunks i.e. chunks with unknown size
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
@Module.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class ChunkAnonService extends Service<ModuleConfig> {

    @Inject
    private BootComponent m_boot;

    @Inject
    private BackupComponent m_backup;

    @Inject
    private ChunkComponent m_chunk;

    @Inject
    private NetworkComponent m_network;

    @Inject
    private LookupComponent m_lookup;

    @Inject
    private NameserviceComponent m_nameservice;

    // chunk operations of service
    private GetAnon m_getAnon;
    private PutAnon m_putAnon;

    /**
     * Get the getAnon operation
     *
     * @return Operation
     */
    public GetAnon getAnon() {
        return m_getAnon;
    }

    /**
     * Get the putAnon operation
     *
     * @return Operation
     */
    public PutAnon putAnon() {
        return m_putAnon;
    }

    @Override
    protected void resolveComponentDependencies(final ComponentProvider p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(BootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_nameservice = p_componentAccessor.getComponent(NameserviceComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        m_getAnon = new GetAnon(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_putAnon = new PutAnon(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
