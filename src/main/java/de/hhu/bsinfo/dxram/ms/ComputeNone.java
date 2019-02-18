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

package de.hhu.bsinfo.dxram.ms;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;

/**
 * None/Null implementation of the compute base. This node does not participate in any
 * master slave computing groups.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
class ComputeNone extends AbstractComputeMSBase {

    /**
     * Constructor
     *
     * @param p_serviceAccessor
     *         Service accessor for tasks.
     * @param p_network
     *         NetworkComponent
     * @param p_nameservice
     *         NameserviceComponent
     * @param p_boot
     *         BootComponent
     * @param p_lookup
     *         LookupComponent
     * @param p_plugin
     *         PluginComponent
     */
    ComputeNone(final DXRAMServiceAccessor p_serviceAccessor, final NetworkComponent p_network,
            final NameserviceComponent p_nameservice, final AbstractBootComponent p_boot,
            final LookupComponent p_lookup, final PluginComponent p_plugin) {
        super(ComputeRole.NONE, (short) -1, 0, p_serviceAccessor, p_network, p_nameservice, p_boot, p_lookup, p_plugin);
    }

    @Override
    public void run() {

    }

    @Override
    public void shutdown() {

    }
}
