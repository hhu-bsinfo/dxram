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

#include "IbDevice.h"

#include "ibnet/sys/Assert.h"
#include "ibnet/sys/Logger.hpp"
#include "ibnet/sys/StringUtils.h"

#include "IbException.h"

#define DEFAULT_IB_PORT 1

namespace ibnet {
namespace core {

const std::string IbDevice::ms_portStateStr[7] {
    "Invalid",
    "Nop",
    "Down",
    "Init",
    "Armed",
    "Active",
    "ActiveDefer"
};

const std::string IbDevice::ms_mtuSizeStr[6] {
    "Invalid",
    "256",
    "512",
    "1024",
    "2048",
    "4096",
};

const std::string IbDevice::ms_linkStateStr[8] {
    "Invalid",
    "Sleep",
    "Polling",
    "Disabled",
    "PortConfTraining",
    "LinkUp",
    "LinkErrRecovery",
    "Phytest"
};

IbDevice::IbDevice(void) :
    m_ibDevGuid((uint64_t) -1),
    m_ibDevName("INVALID"),
    m_lid((uint16_t) -1),
    m_portState(e_PortStateInvalid),
    m_maxMtuSize(e_MtuSizeInvalid),
    m_activeMtuSize(e_MtuSizeInvalid),
    m_linkWidth(e_LinkWidthInvalid),
    m_linkSpeed(e_LinkSpeedInvalid),
    m_linkState(e_LinkStateInvalid),
    m_ibCtx(nullptr)
{
    IBNET_LOG_TRACE_FUNC;

    int num_devices = 0;
    ibv_device** dev_list = nullptr;

    IBNET_LOG_INFO("Opening device...");

    // device enumeration
    IBNET_LOG_TRACE("ibv_get_device_list");
    dev_list = ibv_get_device_list(&num_devices);

    if (dev_list == NULL) {
        IBNET_LOG_ERROR("Getting ib device list: {}", strerror(errno));
        throw IbException("Getting ib device list: " +
            std::string(strerror(errno)));
    }

    if (num_devices == 0) {
        IBNET_LOG_ERROR("Could not find a connected ib device");
        throw IbException("Could not find a connected ib device");
    }

    IBNET_LOG_DEBUG("Found {} ib device(s)", num_devices);

    for (int i = 0; i < num_devices; i++) {
        uint64_t guid = ibv_get_device_guid(dev_list[i]);
        const char* name = ibv_get_device_name(dev_list[i]);

        IBNET_LOG_DEBUG("ibdev {}: {:x} {}", i, guid, name);
    }

    if (num_devices > 1) {
        IBNET_LOG_WARN("Found {} ib devices, using first device", num_devices);
    }

    // default to first found device
    IBNET_LOG_TRACE("ibv_get_device_guid");
    m_ibDevGuid = ibv_get_device_guid(dev_list[0]);
    IBNET_LOG_TRACE("ibv_get_device_name");
    m_ibDevName = ibv_get_device_name(dev_list[0]);

    // open device
    IBNET_LOG_TRACE("ibv_open_device");
    m_ibCtx = ibv_open_device(dev_list[0]);

    if (m_ibCtx == nullptr) {
        IBNET_LOG_ERROR("Opening device {:x} {} failed",
                m_ibDevGuid, m_ibDevName);
        ibv_free_device_list(dev_list);
        throw IbException("Opening device failed");
    }

    // cleanup device list
    IBNET_LOG_TRACE("ibv_free_device_list");
    ibv_free_device_list(dev_list);

    // update once for base information
    UpdateState();

    __LogDeviceAttributes();

    IBNET_LOG_INFO("Opened device {}", *this);
}

IbDevice::~IbDevice(void)
{
    IBNET_LOG_TRACE_FUNC;
    IBNET_ASSERT_PTR(m_ibCtx);

    IBNET_LOG_INFO("Closing device {}", *this);

    IBNET_LOG_TRACE("ibv_close_device");
    ibv_close_device(m_ibCtx);

    m_ibDevGuid = (uint64_t) -1;
    m_ibDevName = "INVALID";
    m_lid = (uint16_t) -1;
    m_linkWidth = e_LinkWidthInvalid;
    m_linkSpeed = e_LinkSpeedInvalid;
    m_linkState = e_LinkStateInvalid;
    m_ibCtx = nullptr;
}

void IbDevice::UpdateState(void)
{
    IBNET_LOG_TRACE_FUNC;
    IBNET_ASSERT_PTR(m_ibCtx);

    struct ibv_port_attr attr;
    int result;

    memset(&attr, 0, sizeof(struct ibv_port_attr));

    result = ibv_query_port(m_ibCtx, DEFAULT_IB_PORT, &attr);

    if (result != 0) {
        IBNET_LOG_ERROR("Querying port for device information failed: {}",
                strerror(result));
        throw IbException("Querying port for device information failed");
    }

    m_lid = attr.lid;

    m_portState = (PortState) attr.state;
    m_maxMtuSize = (MtuSize) attr.max_mtu;
    m_activeMtuSize = (MtuSize) attr.active_mtu;

    switch (attr.active_width) {
        case 1:
            m_linkWidth = e_LinkWidth1X; break;
        case 2:
            m_linkWidth = e_LinkWidth4X; break;
        case 4:
            m_linkWidth = e_LinkWidth8X; break;
        case 8:
            m_linkWidth = e_LinkWidth12X; break;
        default:
            IBNET_ASSERT_DIE("Unhandled switch state");
            break;
    }

    switch (attr.active_speed) {
        case 1:
            m_linkSpeed = e_LinkSpeed2p5; break;
        case 2:
            m_linkSpeed = e_LinkSpeed5; break;
        case 4:
            // also 10 Gbps
        case 8:
            m_linkSpeed = e_LinkSpeed10; break;
        case 16:
            m_linkSpeed = e_LinkSpeed14; break;
        case 32:
            m_linkSpeed = e_LinkSpeed25; break;
        default:
            IBNET_ASSERT_DIE("Unhandled switch state");
            break;
    }

    m_linkState = (LinkState) attr.phys_state;

    if (m_lid == 0) {
        IBNET_LOG_ERROR("Device lid is 0, maybe you forgot to start a "
            "subnet manager?");
    }
}

void IbDevice::__LogDeviceAttributes(void)
{
    struct ibv_device_attr deviceAttr;
    if (ibv_query_device(m_ibCtx, &deviceAttr)) {
        IBNET_LOG_ERROR("Querying device attributes failed: {}",
            std::string(strerror(errno)));
        throw IbException("Querying device attributes failed");
    }

    std::string str =
        "Device attributes:\n"
        "Firmware: " + std::string(deviceAttr.fw_ver) + "\n"
        "GUID: " + sys::StringUtils::ToHexString(deviceAttr.node_guid) + "\n"
        "Sys image GUID: " +
            sys::StringUtils::ToHexString(deviceAttr.sys_image_guid) + "\n"
        "Max memory region size (bytes): " +
            std::to_string(deviceAttr.max_mr_size) + "\n"
        "Max memory page size (bytes): " +
            std::to_string(deviceAttr.page_size_cap) + "\n"
        "Vendor ID: " + std::to_string(deviceAttr.vendor_id) + "\n"
        "Device part ID: " + std::to_string(deviceAttr.vendor_part_id) + "\n"
        "Hardware version: " + std::to_string(deviceAttr.hw_ver) + "\n"
        "Max num QPs: " + std::to_string(deviceAttr.max_qp) + "\n"
        "Max WRQs per QP: " + std::to_string(deviceAttr.max_qp_wr) + "\n"
        "Device capability flags: " +
            std::to_string(deviceAttr.device_cap_flags) + "\n"
        "Max SGEs per WRQs: " + std::to_string(deviceAttr.max_sge) + "\n"
        "Max SGEs per WRQs in RD QP: " +
            std::to_string(deviceAttr.max_sge_rd) + "\n"
        "Max num CQs: " + std::to_string(deviceAttr.max_cq) + "\n"
        "Max elements per CQ: " + std::to_string(deviceAttr.max_cqe) + "\n"
        "Max num memory regions: " + std::to_string(deviceAttr.max_mr) + "\n"
        "Max num prot doms: " + std::to_string(deviceAttr.max_pd) + "\n"
        "max_qp_rd_atom: " + std::to_string(deviceAttr.max_qp_rd_atom) + "\n"
        "max_ee_rd_atom: " + std::to_string(deviceAttr.max_ee_rd_atom) + "\n"
        "max_res_rd_atom: " + std::to_string(deviceAttr.max_res_rd_atom) + "\n"
        "max_qp_init_rd_atom: " +
            std::to_string(deviceAttr.max_qp_init_rd_atom) + "\n"
        "max_ee_init_rd_atom: " +
            std::to_string(deviceAttr.max_ee_init_rd_atom) + "\n"
        "atomic_cap: " + std::to_string(deviceAttr.atomic_cap) + "\n"
        "max_ee: " + std::to_string(deviceAttr.max_ee) + "\n"
        "max_rdd: " + std::to_string(deviceAttr.max_rdd) + "\n"
        "max_mw: " + std::to_string(deviceAttr.max_mw) + "\n"
        "max_raw_ipv6_qp: " + std::to_string(deviceAttr.max_raw_ipv6_qp) + "\n"
        "max_raw_ethy_qp: " + std::to_string(deviceAttr.max_raw_ethy_qp) + "\n"
        "max_mcast_grp: " + std::to_string(deviceAttr.max_mcast_grp) + "\n"
        "max_mcast_qp_attach: " +
            std::to_string(deviceAttr.max_mcast_qp_attach) + "\n"
        "max_total_mcast_qp_attach: " +
            std::to_string(deviceAttr.max_total_mcast_qp_attach) + "\n"
        "max_ah: " + std::to_string(deviceAttr.max_ah) + "\n"
        "max_fmr: " + std::to_string(deviceAttr.max_fmr) + "\n"
        "max_map_per_fmr: " + std::to_string(deviceAttr.max_map_per_fmr) + "\n"
        "Max num of SRQs: " + std::to_string(deviceAttr.max_srq) + "\n"
        "Max num WRQs per SRQ: " + std::to_string(deviceAttr.max_srq_wr) + "\n"
        "Max num SGEs per WRQs on SRQ: " +
            std::to_string(deviceAttr.max_srq_sge) + "\n"
        "max_pkeys: " + std::to_string(deviceAttr.max_pkeys) + "\n"
        "local_ca_ack_delay: " +
            std::to_string(deviceAttr.local_ca_ack_delay) + "\n"
        "phys_port_cnt: " + std::to_string(deviceAttr.phys_port_cnt);

    IBNET_LOG_DEBUG("{}", str);
}

}
}