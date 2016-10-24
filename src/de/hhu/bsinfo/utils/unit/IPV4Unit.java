package de.hhu.bsinfo.utils.unit;

import com.google.gson.annotations.Expose;

import java.net.InetSocketAddress;

/**
 * Wrapper for handling an IPV4 address
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.10.16
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
     * @param p_ip IPV4 address, format xxx.xxx.xxx.xxx
     * @param p_port Port
     */
    public IPV4Unit(final String p_ip, final int p_port) {
        m_ip = p_ip;
        m_port = p_port;
    }

    /**
     * Get the ip address as a string
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
     * @return Ip address and port as string
     */
    public String getAddressStr() {
        return m_ip + ":" + m_port;
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

        return m_ip.equals(obj.getIP()) && m_port == obj.getPort();
    }
}
