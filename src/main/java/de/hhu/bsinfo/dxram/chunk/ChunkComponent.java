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

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;

/**
 * Component class for local chunk handling (access to local key-value memory)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class ChunkComponent extends AbstractDXRAMComponent<ChunkComponentConfig> {
    // component dependencies
    private AbstractBootComponent m_boot;

    private DXMem m_memory;

    /**
     * Constructor
     */
    public ChunkComponent() {
        super(DXRAMComponentOrder.Init.CHUNK, DXRAMComponentOrder.Shutdown.CHUNK, ChunkComponentConfig.class);
    }

    /**
     * Get the local key-value memory instance
     *
     * @return DXMem instance
     */
    public DXMem getMemory() {
        return m_memory;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config, final DXRAMJNIManager p_jniManager) {
        LOGGER.info("Allocating native memory (%d mb). This may take a while...",
                p_config.getComponentConfig(ChunkComponentConfig.class).getKeyValueStoreSize().getMB());

        m_memory = new DXMem(m_boot.getNodeId(),
                p_config.getComponentConfig(ChunkComponentConfig.class).getKeyValueStoreSize().getBytes(),
                p_config.getComponentConfig(ChunkComponentConfig.class).isChunkLockDisabled());

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_memory.shutdown();
        m_memory = null;

        return true;
    }
}
