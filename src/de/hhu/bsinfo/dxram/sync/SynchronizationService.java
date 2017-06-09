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

package de.hhu.bsinfo.dxram.sync;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;

/**
 * Service providing mechanisms for synchronizing.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 06.05.2016
 */
public class SynchronizationService extends AbstractDXRAMService<SynchronizationServiceConfig> {

    // component dependencies
    private LookupComponent m_lookup;

    /**
     * Constructor
     */
    public SynchronizationService() {
        super("sync");
    }

    /**
     * Allocate a barrier for synchronizing multiple peers.
     *
     * @param p_size
     *         Size of the barrier, i.e. number of peers that have to sign on until the barrier gets released.
     * @return Barrier identifier on success, -1 on failure.
     */
    public int barrierAllocate(final int p_size) {
        return m_lookup.barrierAllocate(p_size);
    }

    /**
     * Free an allocated barrier.
     *
     * @param p_barrierId
     *         Barrier to free.
     * @return True if successful, false otherwise.
     */
    public boolean barrierFree(final int p_barrierId) {
        return m_lookup.barrierFree(p_barrierId);
    }

    /**
     * Alter the size of an existing barrier (i.e. you want to keep the barrier id but with a different size).
     *
     * @param p_barrierId
     *         Id of an allocated barrier to change the size of.
     * @param p_newSize
     *         New size for the barrier.
     * @return True if changing size was successful, false otherwise.
     */
    public boolean barrierChangeSize(final int p_barrierId, final int p_newSize) {
        return m_lookup.barrierChangeSize(p_barrierId, p_newSize);
    }

    /**
     * Sign on to a barrier and wait for it getting released (number of peers, barrier size, have signed on).
     *
     * @param p_barrierId
     *         Id of the barrier to sign on to.
     * @param p_customData
     *         Custom data to pass along with the sign on
     * @return BarrierStatus, null on error like barrier does not exist
     */
    public BarrierStatus barrierSignOn(final int p_barrierId, final long p_customData) {
        return m_lookup.barrierSignOn(p_barrierId, p_customData);
    }

    /**
     * Get the status of a specific barrier.
     *
     * @param p_barrierId
     *         Id of the barrier.
     * @return BarrierStatus, null on error like barrier does not exist
     */
    public BarrierStatus barrierGetStatus(final int p_barrierId) {
        return m_lookup.barrierGetStatus(p_barrierId);
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
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
