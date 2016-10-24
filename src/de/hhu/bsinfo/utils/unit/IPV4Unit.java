package de.hhu.bsinfo.utils.unit;

import com.google.gson.annotations.Expose;

/**
 * Wrapper for handling an IPV4 address
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.10.16
 */
public class IPV4Unit {

    @Expose
    private String m_address = "255.255.255.255";
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
     * @param p_address IPV4 address, format xxx.xxx.xxx.xxx
     * @param p_port Port
     */
    public IPV4Unit(final String p_address, final int p_port) {
        m_address = p_address;
        m_port = p_port;
    }

    /**
     * Get the ip address as a string
     * @return IP address as string
     */
    public String getAddress() {
        return m_address;
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
        return m_address + ":" + m_port;
    }
}
