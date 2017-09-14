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

#include <signal.h>

#include <iostream>
#include <chrono>
#include <thread>

#include "ibnet/sys/Logger.h"
#include "ibnet/sys/Network.h"
#include "ibnet/sys/ThreadLoop.h"

#include "ibnet/core/IbConnectionCreatorSimple.h"
#include "ibnet/core/IbConnectionManager.h"
#include "ibnet/core/IbNodeConfArgListReader.h"

class ClientThread : public ibnet::sys::ThreadLoop
{
public:
    ClientThread(uint32_t id, uint16_t ownNodeID,
            const std::vector<std::string>& hostnamesSorted,
             std::shared_ptr<ibnet::core::IbConnectionManager> conMan) :
        ThreadLoop("ClientThread-" + std::to_string(id)),
        m_id(id),
        m_ownNodeId(ownNodeID),
        m_hostnamesSorted(hostnamesSorted),
        m_connectionManager(conMan)
    {};

    virtual ~ClientThread(void)
    {};

protected:
    void _RunLoop(void) override
    {
        // -1: don't count own node
        uint16_t notConnected =
            static_cast<uint16_t>(m_hostnamesSorted.size() - 1);
        uint16_t notConnectedPrev = notConnected;

        for (uint16_t i = 0; i < m_hostnamesSorted.size(); i++) {
            uint16_t remoteNodeId = i;

            if (remoteNodeId == m_ownNodeId) {
                continue;
            }

            try {
                std::shared_ptr<ibnet::core::IbConnection> connection =
                    m_connectionManager->GetConnection(remoteNodeId);

                std::this_thread::sleep_for(std::chrono::milliseconds(100));

                m_connectionManager->ReturnConnection(connection);
                notConnected--;
            } catch (const ibnet::core::IbException& e) {
                std::cout << "!!!!!" << e.what() << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }

        if (m_id == 0 && notConnectedPrev > notConnected) {
            if (notConnected == 0) {
                std::cout << "***** ALL CONNECTED *****" << std::endl;
            } else {
                std::cout << "Waiting for " << std::dec << notConnected <<
                          " more node(s) to connect..." << std::endl;
            }
        }

        if (notConnected == 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

private:
    const uint32_t m_id;
    const uint16_t m_ownNodeId;
    const std::vector<std::string>& m_hostnamesSorted;
    std::shared_ptr<ibnet::core::IbConnectionManager> m_connectionManager;
};

static std::vector<std::unique_ptr<ClientThread>> g_clientThreads;
static bool g_loop = true;

static void SignalHandler(int signal)
{
    if (signal == SIGINT) {
        g_loop = false;
    }
}

int main(int argc, char** argv)
{
    if (argc < 3) {
        printf("Usage: %s <client threads> <hostnames nodes> ...\n", argv[0]);
        return 0;
    }

    backward::SignalHandling sh;

    ibnet::sys::Logger::Setup();

    signal(SIGINT, SignalHandler);

    uint32_t clientThreads = static_cast<uint32_t>(atoi(argv[1]));

    std::shared_ptr<ibnet::core::IbDevice> device =
        std::make_shared<ibnet::core::IbDevice>();

    std::shared_ptr<ibnet::core::IbProtDom> protDom =
        std::make_shared<ibnet::core::IbProtDom>(device, "default");

    uint32_t sizeBuffer = 1000;
    uint8_t* buffer = (uint8_t*) malloc(sizeBuffer);
    memset(buffer, 0, sizeBuffer);

    std::shared_ptr<ibnet::core::IbCompQueue> compQueue =
        std::make_shared<ibnet::core::IbCompQueue>(device, 100);

    std::vector<std::string> hostnamesSorted;
    ibnet::core::IbNodeConfArgListReader nodeConfArgListReader(
        (uint32_t) (argc - 2), &argv[2]);
    ibnet::core::IbNodeConf nodeConf = nodeConfArgListReader.Read();

    for (uint32_t i = 2; i < argc; i++) {
        hostnamesSorted.push_back(std::string(argv[i]));
    }
    std::sort(hostnamesSorted.begin(), hostnamesSorted.end());

    uint16_t ownNodeId = ibnet::core::IbNodeId::INVALID;
    const std::string ownHostname = ibnet::sys::Network::GetHostname();

    uint16_t counter = 0;
    for (auto& it : hostnamesSorted) {
        if (ownHostname == it) {
            ownNodeId = counter;
            break;
        }

        counter++;
    }

    if (ownNodeId == ibnet::core::IbNodeId::INVALID) {
        std::cout << "ERROR Could not assign node id to current host " <<
            ownHostname << ", not found in hostname nodes list" << std::endl;
        return -1;
    }

    std::cout << "Own node id: 0x" <<  std::hex << ownNodeId << std::endl;
    std::shared_ptr<ibnet::core::IbConnectionManager> conMan =
        std::make_shared<ibnet::core::IbConnectionManager>(ownNodeId, nodeConf,
            5731, 1000, 100, device, protDom,
        std::make_unique<ibnet::core::IbConnectionCreatorSimple>(10, 10,
            nullptr, compQueue));

    for (uint32_t i = 0; i < clientThreads; i++) {
        auto thread = std::make_unique<ClientThread>(i, ownNodeId,
            hostnamesSorted, conMan);

        thread->Start();
        g_clientThreads.push_back(std::move(thread));
    }

    std::cout << "Running " << clientThreads << " client threads..." <<
        std::endl;

    while (g_loop) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "Cleanup..." << std::endl;

    for (auto& it : g_clientThreads) {
        it->Stop();
    }

    g_clientThreads.clear();

    conMan.reset();
    compQueue.reset();
    protDom.reset();
    device.reset();

    return 0;
}