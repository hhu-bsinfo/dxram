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

package de.hhu.bsinfo.dxnet.loopback;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Dedicated configuration values for loopback
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 21.09.2017
 */
public class LoopbackConfig {

    @Expose
    private TimeUnit m_requestTimeOut = new TimeUnit(333, TimeUnit.MS);

    @Expose
    private TimeUnit m_connectionTimeOut = new TimeUnit(333, TimeUnit.MS);

    @Expose
    private StorageUnit m_flowControlWindow = new StorageUnit(512, StorageUnit.KB);

    @Expose
    private float m_flowControlWindowThreshold = 0.8f;

    @Expose
    private StorageUnit m_outgoingRingBufferSize = new StorageUnit(2, StorageUnit.MB);

    /**
     * Default constructor
     */
    public LoopbackConfig() {

    }

    /**
     * Amount of time to wait until a request that did not receive a response is considered timed out.
     */
    public TimeUnit getRequestTimeOut() {
        return m_requestTimeOut;
    }

    /**
     * Amount of time to try to establish a connection before giving up
     */
    public TimeUnit getConnectionTimeOut() {
        return m_connectionTimeOut;
    }

    /**
     * Number of bytes to receive on a flow control message before flow control is considered delayed
     */
    public StorageUnit getFlowControlWindow() {
        return m_flowControlWindow;
    }

    /**
     * Get the threshold determining when a flow control message is sent (receivedBytes > m_flowControlWindow * m_flowControlWindowThreshold)
     */
    public float getFlowControlWindowThreshold() {
        return m_flowControlWindowThreshold;
    }

    /**
     * Size of the ring buffer for outgoing network data (per connection)
     */
    public StorageUnit getOugoingRingBufferSize() {
        return m_outgoingRingBufferSize;
    }

    public boolean verify() {
        // TODO
        return true;
    }
}
