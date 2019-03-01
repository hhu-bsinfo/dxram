/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.GetLocal;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Special service with optimized local only operations (does not work with remotely stored chunks).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 19.02.2019
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class ChunkLocalService extends AbstractDXRAMService<DXRAMModuleConfig> {
    // component dependencies
    private AbstractBootComponent<?> m_boot;
    private BackupComponent m_backup;
    private ChunkComponent m_chunk;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;
    private NameserviceComponent m_nameservice;

    // chunk operations of service
    private CreateLocal m_createLocal;
    private GetLocal m_getLocal;
    private ReserveLocal m_reserveLocal;
    private CreateReservedLocal m_createReservedLocal;
    private PinningLocal m_pinningLocal;
    private RawReadLocal m_rawReadLocal;
    private RawWriteLocal m_rawWriteLocal;

    /**
     * Get the CreateLocal operation.
     *
     * @return Operation
     */
    public CreateLocal createLocal() {
        return m_createLocal;
    }

    /**
     * Get the GetLocal operation.
     *
     * @return Operation
     */
    public GetLocal getLocal() {
        return m_getLocal;
    }

    /**
     * Get the ReserveLocal operation.
     *
     * @return Operation
     */
    public ReserveLocal reserveLocal() {
        return m_reserveLocal;
    }

    /**
     * Get the CreateReservedLocal operation.
     *
     * @return Operation
     */
    public CreateReservedLocal createReservedLocal() {
        return m_createReservedLocal;
    }

    /** 
     * Get the PinningLocal operation.
     *
     * @return Operation
     */
    public PinningLocal pinningLocal() {
        return m_pinningLocal;
    }

    /**
     * Get the RawReadLocal operation.
     *
     * @return Operation
     */
    public RawReadLocal rawReadLocal() {
        return m_rawReadLocal;
    }

    /**
     * Get the RawWriteLocal operation.
     *
     * @return Operation
     */
    public RawWriteLocal rawWriteLocal() {
        return m_rawWriteLocal;
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
        m_createLocal = new CreateLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_getLocal = new GetLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_reserveLocal = new ReserveLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_createReservedLocal = 
                new CreateReservedLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_pinningLocal = new PinningLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_rawReadLocal = new RawReadLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_rawWriteLocal = new RawWriteLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}