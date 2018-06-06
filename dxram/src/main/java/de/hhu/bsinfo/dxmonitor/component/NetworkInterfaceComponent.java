/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxmonitor.component;

import de.hhu.bsinfo.dxmonitor.state.NetDevState;

/**
 * Checks if the Network Interface is not in an invalid state by checking if the thresholds have not been exceeded. This class
 * can also be used to get an overview of the component.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class NetworkInterfaceComponent extends AbstractComponent {

    private NetDevState m_lastState;
    private NetDevState m_currState;
    private String m_name;

    private float m_throughputThreshold;
    private float m_errorThreshold;
    private float m_dropThreshold;

    private float m_rpacket;
    private float m_rthroughput;
    private float m_rerror;
    private float m_rdrop;
    private float m_spacket;
    private float m_sthroughput;
    private float m_serror;
    private float m_sdrop;

    /**
     * This constructor should be used in callback mode. All threshold values should be entered in percent (e.g. 0.25
     * 25 %)
     *
     * @param p_name
     *     name of the network interface
     * @param p_secondDelay
     *     refresh rate
     * @param p_throughput
     *     threshold for the througput
     * @param p_error
     *     threshold for faulted packets
     * @param p_drop
     *     threshold for dropped packets
     */
    public NetworkInterfaceComponent(final String p_name, final float p_throughput, final float p_error, final float p_drop, final float p_secondDelay) {
        m_callback = true;
        m_throughputThreshold = p_throughput;
        m_errorThreshold = p_error;
        m_dropThreshold = p_drop;
        m_secondDelay = p_secondDelay;
        m_name = p_name;

        m_currState = new NetDevState(p_name);

        m_startTime = System.currentTimeMillis();
        m_endTime = System.currentTimeMillis();
    }

    /**
     * This constructor should be used in overview mode.
     *
     * @param p_name
     *     name of the network interface
     * @param p_secondDelay
     *     refresh rate
     */
    public NetworkInterfaceComponent(final String p_name, final float p_secondDelay) {
        m_callback = false;
        m_name = p_name;
        m_secondDelay = p_secondDelay;
        m_currState = new NetDevState(p_name);

        m_startTime = System.currentTimeMillis();
        m_endTime = System.currentTimeMillis();
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void analyse() {
        StringBuilder callBack = new StringBuilder("NetIntf:" + m_name + '\n');

        if (m_throughputThreshold - m_rthroughput / 1000.0f / 1000.0f / m_currState.getSpeed() < 0) {
            callBack.append("Receive Throughput Callback");
        }
        if (m_throughputThreshold - m_sthroughput / 1000.0f / 1000.0f / m_currState.getSpeed() < 0) {
            callBack.append("Send Throughput Callback");
        }

        if (m_errorThreshold - m_rerror < 0) {
            callBack.append("Receive Error Callback");
        }
        if (m_errorThreshold - m_serror < 0) {
            callBack.append("Send Error Callback");
        }

        if (m_dropThreshold - m_rdrop < 0) {
            callBack.append("Receive Drop Callback");
        }
        if (m_dropThreshold - m_sdrop < 0) {
            callBack.append("Send Drop Callback");
        }

        String tmp = callBack.toString();
        //if(tmp.equals("NetIntf:"+name+"\n")) output = tmp+"no callbacks!\n";
        //else output = tmp;
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void updateData() {
        m_lastState = m_currState;
        m_currState = new NetDevState(m_name);
        m_endTime = System.currentTimeMillis();
        m_secondDelay = (float) (m_endTime - m_startTime) / 1000.0f;
        m_startTime = m_endTime;
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void calculate() {
        m_rthroughput = 0;
        m_sthroughput = 0;
        //receivestats
        long diff = m_currState.getReceiveBytes() - m_lastState.getReceiveBytes();
        if (diff != 0) {
            m_rthroughput = diff / m_secondDelay / (1000.0f * 1000.0f); // MB/s
            m_rpacket = m_currState.getReceivePackets() - m_lastState.getReceivePackets();
            m_rerror = ((float) m_currState.getReceiveErrors() - m_lastState.getReceiveErrors()) / m_rpacket;
            m_rdrop = ((float) m_currState.getReceiveDrops() - m_lastState.getReceiveDrops()) / m_rpacket;
        }
        //transmitstats
        diff = m_currState.getTransmitBytes() - m_lastState.getTransmitBytes();
        if (diff != 0) {

            m_sthroughput = diff / m_secondDelay / (1000.0f * 1000.0f); // MB/S
            m_spacket = m_currState.getTransmitPackets() - m_lastState.getTransmitPackets();
            m_serror = ((float) m_currState.getTransmitErrors() - m_lastState.getTransmitErrors()) / m_spacket;
            m_sdrop = ((float) m_currState.getTransmitDrops() - m_lastState.getTransmitDrops()) / m_spacket;
        }
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public String toString() {
        return String.format(
            "======== NetInf ========\n" + "Interface: %s\n" + "Receive-Stats - Throughput: %10.2f kb/s\t Packets: %d\t Errors: %d\t Drops: %d\n" +
                "Transmit-Stats - Throughput: %10.2f kb/s\t Packets: %d\t Errors: %d\t Drops: %d\n\n", m_name, m_rthroughput, m_currState.getReceivePackets(),
            m_currState.getReceiveErrors(), m_currState.getReceiveDrops(), m_sthroughput, m_currState.getTransmitPackets(), m_currState.getTransmitErrors(),
            m_currState.getTransmitDrops());
    }

    public float getReceiveThroughput() {
        return m_rthroughput;
    }

    public float getReceiveError() {
        return m_rerror;
    }

    public float getTransmitThroughput() {
        return m_sthroughput;
    }

    public float getTransmitError() {
        return m_serror;
    }

}
