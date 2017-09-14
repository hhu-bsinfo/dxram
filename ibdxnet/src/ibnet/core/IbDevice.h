/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf,
 * Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

#ifndef IBNET_CORE_IBDEVICE_H
#define IBNET_CORE_IBDEVICE_H

#include <iostream>
#include <string>

#include <infiniband/verbs.h>
#include <spdlog/fmt/ostr.h>

namespace ibnet {
namespace core {

/**
 * Wrapper class for an InfiniBand device. Wraps various setup calls to
 * open an InfiniBand device and setup the InfiniBand context for further
 * operation.
 *
 * Parts of the documentation were copied from:
 * http://www.rdmamojo.com/2012/07/21/ibv_query_port/
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbDevice
{
public:
    /**
     * Current state of the port of an InfiniBand HCA
     */
    enum PortState
    {
        e_PortStateInvalid = -1,

        /**
         * Reserved value, which shouldn't be observed
         */
        e_PortStateNop = 0,

        /**
         * Logical link is down. The physical link of the port isn't up.
         * In this state, the link layer discards all packets presented to
         * it for transmission
         */
        e_PortStateDown = 1,

        /**
         * Logical link is Initializing. The physical link of the port is up,
         * but the SM haven't yet configured the logical link. In this state,
         * the link layer can only receive and transmit SMPs and flow control
         * link packets, other types of packets presented to it for
         * transmission are discarded
         */
        e_PortStateInit = 2,

        /**
         * Logical link is Armed. The physical link of the port is up,
         * but the SM haven't yet fully configured the logical link. In this
         * state, the link layer can receive and transmit SMPs and flow control
         * link packets, other types of packets can be received, but discards
         * non SMP packets for sending
         */
        e_PortStateArmed = 3,

        /**
         * Logical link is Active. The link layer can transmit and receive
         * all packet types
         */
        e_PortStateActive = 4,

        /**
         * Logical link is in Active Deferred. The logical link was Active,
         * but the physical link suffered from a failure. If the error will
         * be recovered within a timeout, the logical link will return to
         */
        e_PortStateActiveDefer = 5,
    };

    /**
     * Possible MTU sizes in bytes (support depends on the HCA used)
     */
    enum MtuSize
    {
        e_MtuSizeInvalid = 0,
        e_MtuSize256 = 1,
        e_MtuSize512 = 2,
        e_MtuSize1024 = 3,
        e_MtuSize2048 = 4,
        e_MtuSize4096 = 5,
    };

    /**
     * Link width of a port
     */
    enum LinkWidth
    {
        e_LinkWidthInvalid = 0,
        e_LinkWidth1X = 1,
        e_LinkWidth4X = 4,
        e_LinkWidth8X = 8,
        e_LinkWidth12X = 12
    };

    /**
     * Active link speed of a port in Gbps
     */
    enum LinkSpeed
    {
        e_LinkSpeedInvalid = 0,
        e_LinkSpeed2p5 = 25,
        e_LinkSpeed5 = 50,
        e_LinkSpeed10 = 100,
        e_LinkSpeed14 = 140,
        e_LinkSpeed25 = 250
    };

    /**
     * Physical link state of a port
     */
    enum LinkState
    {
        e_LinkStateInvalid = 0,

        /**
         * The port drives its output to quiescent levels and does not respond
         * to received data. In this state, the link is deactivated
         * without powering off the port
         */
        e_LinkStateSleep = 1,

        /**
         * The port transmits training sequences and responds to receive
         * training sequences.
         */
        e_LinkStatePolling = 2,

        /**
         * The port drives its output to quiescent levels and does not
         * respond to receive data
         */
        e_LinkStateDisabled = 3,

        /**
         * Both transmitter and receive active and the port is attempting to
         * configure and transition to the LinkUp state
         */
        e_LinkStatePortConfTrain = 4,

        /**
         * The port is available to send and receive packets
         */
        e_LinkStateLinkUp = 5,

        /**
         * Port attempts to re-synchronize the link and return it to
         * normal operation
         */
        e_LinkStateLinkErrRecovery = 6,

        /**
         * Port allows the transmitter and received circuitry to be tested by
         * external test equipment for compliance with the transmitter
         * and receiver specifications
         */
        e_LinkStatePhytest = 7
    };

    /**
     * Constructor
     *
     * Opens the first InfiniBand device found or throws an exception if no
     * device found.
     */
    IbDevice(void);

    /**
     * Destructor
     *
     * Close InfiniBand device and cleanup resources
     */
    ~IbDevice(void);

    /**
     * Update the device state. This updates some variables that
     * provide information about the current state of the opened device. If
     * you query these variables, ensure to call this update function
     * frequently.
     */
    void UpdateState(void);

    /**
     * Get the GUID of the device
     */
    uint64_t GetGuid(void) const {
        return m_ibDevGuid;
    }

    /**
     * Get the name of the device
     */
    const std::string& GetName(void) const {
        return m_ibDevName;
    }

    /**
     * Get the device's LID
     */
    uint16_t GetLid(void) const {
        return m_lid;
    }

    /**
     * Get the link's width
     */
    LinkWidth GetLinkWidth(void) const {
        return m_linkWidth;
    }

    /**
     * Get the link's speed
     */
    LinkSpeed GetLinkSpeed(void) const {
        return m_linkSpeed;
    }

    /**
     * Get the current state of the link
     */
    LinkState GetLinkState(void) const {
        return m_linkState;
    }

    /**
     * Get the InfiniBand context provided by the opened device
     */
    ibv_context* GetIBContext(void) const {
        return m_ibCtx;
    }

    /**
     * Enable output to an out stream
     */
    friend std::ostream &operator<<(std::ostream& os, const IbDevice& o) {
        return os << "0x" << std::hex << o.m_ibDevGuid
                  << ", " << o.m_ibDevName
                  << ", " << std::hex << "0x" << o.m_lid
                  << ", " << o.m_linkWidth << "X"
                  << ", " << o.m_linkSpeed / 10.f << " gbps"
                  << ", MaxMTU " << ms_mtuSizeStr[o.m_maxMtuSize]
                  << ", ActiveMTU " << ms_mtuSizeStr[o.m_maxMtuSize]
                  << ", Port " << ms_portStateStr[o.m_portState]
                  << ", Link " << ms_linkStateStr[o.m_linkState];
    }

private:
    static const std::string ms_portStateStr[7];
    static const std::string ms_mtuSizeStr[6];
    static const std::string ms_linkStateStr[8];

private:
    uint64_t m_ibDevGuid;
    std::string m_ibDevName;
    uint16_t m_lid;

    PortState m_portState;
    MtuSize m_maxMtuSize;
    MtuSize m_activeMtuSize;
    LinkWidth m_linkWidth;
    LinkSpeed m_linkSpeed;
    LinkState m_linkState;

    ibv_context* m_ibCtx;

    void __LogDeviceAttributes(void);
};

}
}

#endif // IBNET_CORE_IBDEVICE_H
