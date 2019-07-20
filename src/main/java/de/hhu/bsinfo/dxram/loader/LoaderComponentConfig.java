/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
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

package de.hhu.bsinfo.dxram.loader;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;

/**
 * @author Julien Bernhart, julien.bernhart@hhu.de, 2019-04-17
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class LoaderComponentConfig extends ModuleConfig {

    /**
     * Enable or disable random request: if false, peers contact the responsible superpeer, if true, peers contact a
     * random superpeer
     */
    @Expose
    private boolean m_randomRequest = false;

    /**
     * If true, clients get the newest version of each application without request
     */
    @Expose
    private boolean m_autoUpdate = true;

    /**
     * The path where loaded Jars get stored.
     */
    @Expose
    private final String m_loaderDir = "loadedJars";

    /**
     * The number of tries a peer executes getJar before a ClassNotFoundException is thrown.
     */
    @Expose
    private final int m_maxTries = 2;

    /**
     * The time a peer is waiting, before a retry is executed, in milliseconds.
     */
    @Expose
    public final int m_retryInterval = 500;

    /**
     * If true, a superpeer forces a synchronisation,
     * if a ClassRequests requests a class, that is not locally available.
     */
    @Expose
    public final boolean m_forceSyncWhenNotFound = true;


    public LoaderComponentConfig() {
        super(LoaderComponent.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        return true;
    }
}
