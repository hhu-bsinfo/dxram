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

package de.hhu.bsinfo.dxutils.unit;

import java.net.InetSocketAddress;

import com.google.gson.annotations.Expose;

/**
 * Wrapper for handling an IPV4 address
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.10.2016
 */
public class IPV4Unit {

    @Expose
    private String m_ip = "255.255.255.255";
    @Expose
    private int m_port = 65535;

    /**
     * Constructor
     */
    public IPV4Unit() {

    }

    /**
     * Constructor
     *
     * @param p_ip
     *     IPV4 address, format xxx.xxx.xxx.xxx
     * @param p_port
     *     Port
     */
    public IPV4Unit(final String p_ip, final int p_port) {
        m_ip = p_ip;
        m_port = p_port;
    }

    /**
     * Get the ip address as a string
     *
     * @return IP address as string
     */
    public String getIP() {
        return m_ip;
    }

    /**
     * Get the port
     *
     * @return POrt
     */
    public int getPort() {
        return m_port;
    }

    /**
     * Get the ip address and port as a string of format xxx.xxx.xxx.xxx:xxxxx
     *
     * @return Ip address and port as string
     */
    public String getAddressStr() {
        return m_ip + ':' + m_port;
    }

    /**
     * Get as InetSocketAddress object
     *
     * @return InetSocketAddress
     */
    public InetSocketAddress getInetSocketAddress() {
        return new InetSocketAddress(m_ip, m_port);
    }

    @Override
    public String toString() {
        return getAddressStr();
    }

    @Override
    public boolean equals(final Object p_obj) {
        if (!(p_obj instanceof IPV4Unit)) {
            return false;
        }

        if (p_obj == this) {
            return true;
        }

        IPV4Unit obj = (IPV4Unit) p_obj;

        return m_ip.equals(obj.m_ip) && m_port == obj.m_port;
    }

    @Override
    public int hashCode() {
        return (m_ip + '/' + m_port).hashCode();
    }
}
