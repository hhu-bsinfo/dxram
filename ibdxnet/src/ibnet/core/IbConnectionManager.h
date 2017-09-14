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

#ifndef IBNET_CORE_IBCONNECTIONMANAGER_H
#define IBNET_CORE_IBCONNECTIONMANAGER_H

#include <atomic>
#include <mutex>
#include <unordered_map>

#include "ibnet/sys/SocketUDP.h"
#include "ibnet/sys/ThreadLoop.h"

#include "IbCompQueue.h"
#include "IbConnection.h"
#include "IbConnectionCreator.h"
#include "IbConnectionManagerJobQueue.h"
#include "IbNodeConf.h"
#include "IbNodeId.h"

namespace ibnet {
namespace core {

class IbConnectionManager
{
public:
    class Listener
    {
    public:
        Listener(void) {};
        virtual ~Listener(void) {};

        virtual void NodeDiscovered(uint16_t nodeId) = 0;

        virtual void NodeInvalidated(uint16_t nodeId) = 0;

        virtual void NodeConnected(uint16_t nodeId,
                                   IbConnection& connection) = 0;

        virtual void NodeDisconnected(uint16_t nodeId) = 0;
    };

private:
    static const uint32_t MAX_QPS_PER_CONNECTION = 32;
    static const int32_t CONNECTION_NOT_AVAILABLE = INT32_MIN;
    static const int32_t CONNECTION_AVAILABLE = 0;
    static const int32_t CONNECTION_CLOSING = INT32_MIN / 2;

private:
    class ConnectionContext;
    class ExchangeThread;
    class JobThread;

    class DiscoveryContext
    {
    public:
        DiscoveryContext(uint16_t ownNodeId, const IbNodeConf& nodeConf,
                         ExchangeThread& exchangeThread, JobThread& jobThread);
        ~DiscoveryContext(void);

        void AddNode(const IbNodeConf::Entry& entry);

        std::shared_ptr<IbNodeConf::Entry> GetNodeInfo(uint16_t nodeId);

        void SetListener(Listener* listener) {
            m_listener = listener;
        }

        void Discover(void);

        void Discovered(uint16_t nodeId, uint32_t remoteIpAddr);

        void Invalidate(uint16_t nodeId, bool shutdown);

    private:
        const uint16_t m_ownNodeId;

        Listener* m_listener;

        ExchangeThread& m_exchangeThread;
        JobThread& m_jobThread;

        std::mutex m_lock;
        std::vector<std::shared_ptr<IbNodeConf::Entry>> m_infoToGet;
        std::shared_ptr<IbNodeConf::Entry> m_nodeInfo[IbNodeId::MAX_NUM_NODES];
    };

    class ConnectionContext
    {
    public:
        ConnectionContext(uint16_t ownNodeId,
                          uint32_t connectionCreationTimeoutMs,
                          uint32_t maxNumConnections,
                          DiscoveryContext& discoveryContext,
                          ExchangeThread& exchangeThread, JobThread& jobThread,
                          std::shared_ptr<IbDevice> device,
                          std::shared_ptr<IbProtDom> protDom,
                          std::unique_ptr<IbConnectionCreator> connectionCreator);
        ~ConnectionContext(void);

        void SetListener(Listener* listener) {
            m_listener = listener;
        }

        /**
         * Get the (remote) node id of a node with a physical QP num
         *
         * @param qpNum QP num of the node to get the node id of
         * @return Node id of the node owning the QP with the physical QP id
         */
        uint16_t GetNodeIdForPhysicalQPNum(uint32_t qpNum);

        /**
         * Check if a connection is available (open and connected)
         *
         * @param destination Remote node id
         * @return True if available, false otherwise
         */
        bool IsConnectionAvailable(uint16_t nodeId);

        /**
         * Get a connection
         *
         * If called for the first time with a node id, this might establish the
         * connection to the specified node id but keeps it opened either until
         * closed or the max number of connections is exceeded and the connection
         * must be suppressed.
         *
         * Use the returned shared pointer as a handle to determine who is still
         * owning a reference to the connection.
         *
         * @param nodeId Get the connection of this node
         * @return If successful, a valid pointer to the established connection.
         *          Throws exceptions on errors.
         */
        std::shared_ptr<IbConnection> GetConnection(uint16_t nodeId);

        /**
         * Return a connection that was retrieved on the GetConnection call.
         * This MUST be called for every GetConnection call to ensure
         * consistency with keeping track of threads working on connections.
         *
         * @param connection Connection to return
         */
        void ReturnConnection(std::shared_ptr<IbConnection>& connection);

        /**
         * Explicitly close a connection
         *
         * @param nodeId Node id of the connection to close
         * @param force True to force close (don't wait for queues to be emptied),
         *          false to empty the queues and ensure everything queued is still
         *          being processed/
         */
        void CloseConnection(uint16_t nodeId, bool force);

    public:
        void Create(uint16_t nodeId, uint32_t ident, uint16_t lid,
                    uint32_t* physicalQpIds);

        void Close(uint16_t nodeId, bool force, bool shutdown);

    private:
        const uint16_t m_ownNodeId;
        const uint32_t m_connectionCtxIdent;
        const uint32_t m_connectionCreationTimeoutMs;
        const uint32_t m_maxNumConnections;

        Listener* m_listener;

        DiscoveryContext& m_discoveryContext;
        ExchangeThread& m_exchangeThread;
        JobThread& m_jobThread;

        std::shared_ptr<IbDevice> m_device;
        std::shared_ptr<IbProtDom> m_protDom;

        std::atomic<int32_t> m_connectionAvailable[IbNodeId::MAX_NUM_NODES];
        std::unique_ptr<IbConnectionCreator> m_connectionCreator;
        std::shared_ptr<IbConnection> m_connections[IbNodeId::MAX_NUM_NODES];
        uint32_t m_openConnections;

        std::vector<uint16_t> m_availableConnectionIds;

        std::unordered_map<uint32_t, uint16_t> m_qpNumToNodeIdMappings;
    };

    class ExchangeThread : public sys::ThreadLoop
    {
    public:
        ExchangeThread(uint16_t ownNodeId, uint16_t socketPort,
                       JobThread& jobThread);
        ~ExchangeThread(void);

        void SendDiscoveryReq(uint16_t ownNodeId, uint32_t destIp);

        void SendExchgInfo(uint16_t nodeId, uint32_t ident, uint16_t lid,
            uint32_t* physicalQpIds, uint32_t destIp);

    protected:
        void _RunLoop(void) override;

    private:
        enum PaketType
        {
            PT_NODE_DISCOVERY_REQ = 0,
            PT_NODE_DISCOVERY_RESP = 1,
            PT_NODE_CON_INFO = 2,
        };

        struct Paket
        {
            uint32_t m_magic;
            PaketType m_type;
            uint16_t m_nodeId;
            uint32_t m_ident;
            uint16_t m_lid;
            uint32_t m_physicalQpId[MAX_QPS_PER_CONNECTION];
        };

    private:
        const uint16_t m_ownNodeId;

        std::unique_ptr<sys::SocketUDP> m_socket;
        Paket m_recvPaket;

        JobThread& m_jobThread;

        bool m_noDataAvailable;

    private:
        bool __SendDiscoveryResp(uint16_t ownNodeId, uint32_t destIp);
    };

    class JobThread : public sys::ThreadLoop
    {
    public:
        JobThread(DiscoveryContext& discoveryContext,
                  ConnectionContext& conncectionContext);
        ~JobThread(void);

        void AddCreateJob(uint16_t nodeId);

        void AddCreateJob(uint16_t nodeId, uint32_t ident, uint16_t lid,
            uint32_t* physicalQpIds);

        void AddCloseJob(uint16_t nodeId, bool force, bool shutdown);

        void AddDiscoverJob(void);

        void AddDiscoveredJob(uint16_t nodeId, uint32_t remoteIpAddr);

        bool IsQueueEmpty(void);

    protected:
        void _RunLoop(void) override;

    private:
        IbConnectionManagerJobQueue m_queue;
        IbConnectionManagerJobQueue::Job m_job;
        std::atomic<bool> m_runDiscovery;

        DiscoveryContext& m_discoveryContext;
        ConnectionContext& m_connectionContext;

        void __AddJob(IbConnectionManagerJobQueue::Job& job);
    };

public:
    IbConnectionManager(uint16_t ownNodeId, const IbNodeConf& nodeConf,
                         uint16_t socketPort,
                         uint32_t connectionCreationTimeoutMs,
                         uint32_t maxNumConnections,
                         std::shared_ptr<IbDevice>& device,
                         std::shared_ptr<IbProtDom>& protDom,
                         std::unique_ptr<IbConnectionCreator> connectionCreator);

    ~IbConnectionManager(void);

    /**
     * Add another node to the manager to allow discovery
     *
     * @param entry NodeConf entry to add to the existing config used by the
     *          manager
     */
    void AddNode(const IbNodeConf::Entry& entry);

    /**
     * Set a connection listener which listens to node connect/disconnect
     * events
     *
     * @param listener Listener to set
     */
    void SetNodeConnectedListener(Listener* listener) {
        m_discoveryContext.SetListener(listener);
        m_connectionContext.SetListener(listener);
    }

    /**
     * Get the (remote) node id of a node with a physical QP num
     *
     * @param qpNum QP num of the node to get the node id of
     * @return Node id of the node owning the QP with the physical QP id
     */
    uint16_t GetNodeIdForPhysicalQPNum(uint32_t qpNum) {
        return m_connectionContext.GetNodeIdForPhysicalQPNum(qpNum);
    }

    /**
     * Check if a connection is available (open and connected)
     *
     * @param destination Remote node id
     * @return True if available, false otherwise
     */
    bool IsConnectionAvailable(uint16_t nodeId);

    /**
     * Get a connection
     *
     * If called for the first time with a node id, this might establish the
     * connection to the specified node id but keeps it opened either until
     * closed or the max number of connections is exceeded and the connection
     * must be suppressed.
     *
     * Use the returned shared pointer as a handle to determine who is still
     * owning a reference to the connection.
     *
     * @param nodeId Get the connection of this node
     * @return If successful, a valid pointer to the established connection.
     *          Throws exceptions on errors.
     */
    std::shared_ptr<IbConnection> GetConnection(uint16_t nodeId);

    /**
     * Return a connection that was retrieved on the GetConnection call.
     * This MUST be called for every GetConnection call to ensure
     * consistency with keeping track of threads working on connections.
     *
     * @param connection Connection to return
     */
    void ReturnConnection(std::shared_ptr<IbConnection>& connection);

    /**
     * Explicitly close a connection
     *
     * @param nodeId Node id of the connection to close
     * @param force True to force close (don't wait for queues to be emptied),
     *          false to empty the queues and ensure everything queued is still
     *          being processed/
     */
    void CloseConnection(uint16_t nodeId, bool force);

private:
    DiscoveryContext m_discoveryContext;
    ConnectionContext m_connectionContext;

    ExchangeThread m_exchangeThread;
    JobThread m_jobThread;
};

}
}

#endif //IBNET_CORE_IBCONNECTIONMANAGER_H
