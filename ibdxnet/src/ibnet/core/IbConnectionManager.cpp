#include "IbConnectionManager.h"

#include "ibnet/sys/Network.h"
#include "ibnet/sys/Random.h"

#include "ibnet/core/IbTimeoutException.h"

namespace ibnet {
namespace core {

#define PAKET_MAGIC 0xBEEFCA4E

IbConnectionManager::IbConnectionManager(uint16_t ownNodeId,
        const IbNodeConf& nodeConf, uint16_t socketPort,
        uint32_t connectionCreationTimeoutMs, uint32_t maxNumConnections,
        std::shared_ptr<IbDevice>& device, std::shared_ptr<IbProtDom>& protDom,
        std::unique_ptr<IbConnectionCreator> connectionCreator) :
    m_discoveryContext(ownNodeId, nodeConf, m_exchangeThread, m_jobThread),
    m_connectionContext(ownNodeId, connectionCreationTimeoutMs,
        maxNumConnections, m_discoveryContext, m_exchangeThread, m_jobThread,
        device, protDom, std::move(connectionCreator)),
    m_exchangeThread(ownNodeId, socketPort, m_jobThread),
    m_jobThread(m_discoveryContext, m_connectionContext)
{
    IBNET_LOG_TRACE_FUNC;
    IBNET_LOG_INFO("Starting connection manager...");

    m_exchangeThread.Start();
    m_jobThread.Start();

    // add initial discovery job to get everything started
    m_jobThread.AddDiscoverJob();
}

IbConnectionManager::~IbConnectionManager(void)
{
    IBNET_LOG_TRACE_FUNC;
    IBNET_LOG_INFO("Shutting down connection manager...");

    // close opened connections
    for (uint32_t i = 0; i < IbNodeId::MAX_NUM_NODES; i++) {
        if (m_connectionContext.IsConnectionAvailable(
                static_cast<uint16_t>(i))) {
            m_jobThread.AddCloseJob(static_cast<uint16_t>(i), true, true);
        }
    }

    // wait until all jobs are processed
    while (!m_jobThread.IsQueueEmpty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    m_jobThread.Stop();
    m_exchangeThread.Stop();

    IBNET_LOG_DEBUG("Shutting down connection manager done");
}

void IbConnectionManager::AddNode(const IbNodeConf::Entry& entry)
{
    m_discoveryContext.AddNode(entry);
}

bool IbConnectionManager::IsConnectionAvailable(uint16_t nodeId)
{
    return m_connectionContext.IsConnectionAvailable(nodeId);
}

std::shared_ptr<IbConnection> IbConnectionManager::GetConnection(
        uint16_t nodeId)
{
    return m_connectionContext.GetConnection(nodeId);
}

void IbConnectionManager::ReturnConnection(
        std::shared_ptr<IbConnection>& connection)
{
    m_connectionContext.ReturnConnection(connection);
}

void IbConnectionManager::CloseConnection(uint16_t nodeId, bool force)
{
    m_connectionContext.CloseConnection(nodeId, force);
}

IbConnectionManager::DiscoveryContext::DiscoveryContext(uint16_t ownNodeId,
        const IbNodeConf& nodeConf, ExchangeThread& exchangeThread,
        JobThread& jobThread) :
    m_ownNodeId(ownNodeId),
    m_listener(nullptr),
    m_exchangeThread(exchangeThread),
    m_jobThread(jobThread)
{
    IBNET_LOG_INFO("Initializing node discovery list, own node id 0x{:x}...",
        ownNodeId);

    std::string ownHostname = sys::Network::GetHostname();

    for (auto& it : nodeConf.GetEntries()) {

        // don't add self
        if (it.GetHostname() != ownHostname) {
            m_infoToGet.push_back(std::make_shared<IbNodeConf::Entry>(it));
        }
    }
}

IbConnectionManager::DiscoveryContext::~DiscoveryContext(void)
{

}

void IbConnectionManager::DiscoveryContext::AddNode(
        const IbNodeConf::Entry& entry)
{
    IBNET_LOG_TRACE_FUNC;

    std::string ownHostname = sys::Network::GetHostname();

    // don't add ourselves
    if (entry.GetHostname() != ownHostname) {
        IBNET_LOG_INFO("Adding node {}", entry);

        std::lock_guard<std::mutex> l(m_lock);
        m_infoToGet.push_back(std::make_shared<IbNodeConf::Entry>(entry));
    }

    // trigger discovery
    m_jobThread.AddDiscoverJob();
}

std::shared_ptr<IbNodeConf::Entry>
        IbConnectionManager::DiscoveryContext::GetNodeInfo(uint16_t nodeId)
{
    IBNET_LOG_TRACE_FUNC;

    std::lock_guard<std::mutex> l(m_lock);

    if (!m_nodeInfo[nodeId]) {
        throw IbNodeNotAvailableException(nodeId);
    }

    return m_nodeInfo[nodeId];
}

void IbConnectionManager::DiscoveryContext::Discover(void)
{
    IBNET_LOG_TRACE("Requesting node info of {} nodes", m_infoToGet.size());

    m_lock.lock();

    // request remote node's information if not received, yet
    for (auto& it : m_infoToGet) {
        IBNET_LOG_TRACE("Requesting node info from {}",
            it->GetAddress().GetAddressStr());

        m_exchangeThread.SendDiscoveryReq(m_ownNodeId,
            it->GetAddress().GetAddress());
    }

    m_lock.unlock();

    // there are more nodes to be discovered
    if (m_infoToGet.size() != 0) {
        m_jobThread.AddDiscoverJob();
    }

    // reduce cpu load
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
}

void IbConnectionManager::DiscoveryContext::Discovered(uint16_t nodeId,
        uint32_t remoteIpAddr)
{
    bool found = false;

    m_lock.lock();

    // remove from processing list
    for (auto it = m_infoToGet.begin(); it != m_infoToGet.end(); it++) {
        if ((*it)->GetAddress().GetAddress() == remoteIpAddr) {
            IBNET_LOG_INFO("Discovered node {} as node id 0x{:x}",
                (*it)->GetAddress().GetAddressStr(), nodeId);

            // store remote node information
            m_nodeInfo[nodeId] = *it;

            m_infoToGet.erase(it);

            m_lock.unlock();

            found = true;

            // don't lock call to listener
            if (m_listener) {
                m_listener->NodeDiscovered(nodeId);
            }

            break;
        }
    }

    if (!found) {
        m_lock.unlock();
    }

}

void IbConnectionManager::DiscoveryContext::Invalidate(uint16_t nodeId,
        bool shutdown)
{
    m_lock.lock();
    m_infoToGet.push_back(m_nodeInfo[nodeId]);
    m_nodeInfo[nodeId].reset();
    m_lock.unlock();

    // add job to re-discover if system is not shutting down
    if (!shutdown) {
        m_jobThread.AddDiscoverJob();
    }

    if (m_listener) {
        m_listener->NodeInvalidated(nodeId);
    }
}

IbConnectionManager::ConnectionContext::ConnectionContext(uint16_t ownNodeId,
        uint32_t connectionCreationTimeoutMs, uint32_t maxNumConnections,
        DiscoveryContext& discoveryContext, ExchangeThread& exchangeThread,
        JobThread& jobThread, std::shared_ptr<IbDevice> device,
        std::shared_ptr<IbProtDom> protDom,
        std::unique_ptr<IbConnectionCreator> connectionCreator) :
    m_ownNodeId(ownNodeId),
    m_connectionCtxIdent(sys::Random::Generate32()),
    m_connectionCreationTimeoutMs(connectionCreationTimeoutMs),
    m_maxNumConnections(maxNumConnections),
    m_listener(nullptr),
    m_discoveryContext(discoveryContext),
    m_exchangeThread(exchangeThread),
    m_jobThread(jobThread),
    m_device(device),
    m_protDom(protDom),
    m_connectionCreator(std::move(connectionCreator)),
    m_openConnections(0)
{
    IBNET_LOG_TRACE_FUNC;

    if (ownNodeId == IbNodeId::INVALID) {
        throw IbException("Invalid node id provided");
    }

    for (uint32_t i = 0; i < IbNodeId::MAX_NUM_NODES; i++) {
        m_connectionAvailable[i].store(CONNECTION_NOT_AVAILABLE,
            std::memory_order_relaxed);
    }

    // fill array with 'unique' connection ids
    // ids are reused to ensure the max id is max_connections - 1
    for (int i = m_maxNumConnections - 1; i >= 0; i--) {
        uint16_t tmp = (uint16_t) i;
        m_availableConnectionIds.push_back(tmp);
    }
}

IbConnectionManager::ConnectionContext::~ConnectionContext(void)
{

}

uint16_t IbConnectionManager::ConnectionContext::GetNodeIdForPhysicalQPNum(
        uint32_t qpNum) {
    auto it = m_qpNumToNodeIdMappings.find(qpNum);

    if (it != m_qpNumToNodeIdMappings.end()) {
        return it->second;
    }

    return IbNodeId::INVALID;
}

bool IbConnectionManager::ConnectionContext::IsConnectionAvailable(
        uint16_t nodeId) {
    return m_connectionAvailable[nodeId].load(std::memory_order_relaxed) >=
           CONNECTION_AVAILABLE;
}

std::shared_ptr<IbConnection> IbConnectionManager::ConnectionContext::GetConnection(
        uint16_t nodeId)
{
    if (nodeId == IbNodeId::INVALID) {
        throw IbException("Invalid node id provided");
    }

    // keep track of "handles" issued
    int32_t available = m_connectionAvailable[nodeId].fetch_add(1,
        std::memory_order_acquire);

    if (available >= CONNECTION_AVAILABLE) {
        return m_connections[nodeId];
    }

    IBNET_LOG_TRACE("GetConnection: 0x{:x}, avail: {}", nodeId, available + 1);

    m_jobThread.AddCreateJob(nodeId);

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point end;

    start = std::chrono::high_resolution_clock::now();
    do {
        if (m_connectionAvailable[nodeId].load(std::memory_order_acquire) >=
                CONNECTION_AVAILABLE) {
            available = m_connectionAvailable[nodeId].fetch_add(1,
                std::memory_order_acquire);

            if (available >= CONNECTION_AVAILABLE) {
                // sanity check
                if (m_connections[nodeId] == nullptr) {
                    throw IbException("Invalid connection state on "
                        "GetConnection");
                }

                return m_connections[nodeId];
            }
        }

        std::this_thread::yield();

        end = std::chrono::high_resolution_clock::now();
    } while (end - start <
        std::chrono::milliseconds(m_connectionCreationTimeoutMs));


    std::chrono::duration<uint64_t, std::nano> delta(end - start);
    throw IbTimeoutException(nodeId, "Creating connection: " +
        std::to_string(delta.count() / 1000 / 1000) + " ms");
}

void IbConnectionManager::ConnectionContext::ReturnConnection(
        std::shared_ptr<IbConnection>& connection)
{
    int32_t tmp = m_connectionAvailable[connection->GetRemoteNodeId()]
        .fetch_sub(1, std::memory_order_relaxed);

    IBNET_LOG_TRACE("ReturnConnection: 0x{:x}, avail {}",
        connection->GetRemoteNodeId(), tmp - 1);
}

void IbConnectionManager::ConnectionContext::CloseConnection(uint16_t nodeId,
        bool force)
{
    m_jobThread.AddCloseJob(nodeId, force, false);
}

void IbConnectionManager::ConnectionContext::Create(uint16_t nodeId,
        uint32_t ident, uint16_t lid, uint32_t* physicalQpIds)
{
    IBNET_LOG_TRACE_FUNC;

    std::shared_ptr<IbNodeConf::Entry> remoteNodeInfo;

    try {
        // get remote node connection information
        remoteNodeInfo = m_discoveryContext.GetNodeInfo(nodeId);
    } catch (...) {
        IBNET_LOG_WARN("Cannot create connection to remote 0x{:X}, "
            "not discovered, yet", nodeId);
        return;
    }

    // allocate connection if necessary
    if (m_connections[nodeId] == nullptr) {
        uint16_t connectionId = m_availableConnectionIds.back();
        m_availableConnectionIds.pop_back();

        m_connections[nodeId] = m_connectionCreator->CreateConnection(
            connectionId, m_device, m_protDom);

        for (auto& it : m_connections[nodeId]->GetQps()) {
            m_qpNumToNodeIdMappings.insert(std::make_pair(
                it->GetPhysicalQpNum(), nodeId));
        }

        if (m_connections[nodeId]->GetQps().size() > MAX_QPS_PER_CONNECTION) {
            throw IbException("Exceeded max qps per connection limit");
        }

        IBNET_LOG_DEBUG("Allocated new connection to remote 0x{:X} with {} QPs",
            nodeId, m_connections[nodeId]->GetQps().size());
    }

    // not connected (yet) and remote QP ctx available -> finish connection
    if (!m_connections[nodeId]->IsConnected() && lid != 0xFFFF) {
        std::vector<uint32_t> remotePhysicalQpIds;
        for (uint32_t i = 0; i < MAX_QPS_PER_CONNECTION; i++) {
            if (physicalQpIds[i] == 0xFFFFFFFF) {
                break;
            }

            remotePhysicalQpIds.push_back(physicalQpIds[i]);
        }

        IbRemoteInfo remoteInfo(nodeId, lid, ident, remotePhysicalQpIds);

        m_connections[nodeId]->Connect(remoteInfo);
        IBNET_LOG_INFO("Connected QP to remote {}", remoteInfo);
        m_openConnections++;

        m_connectionAvailable[nodeId].store(CONNECTION_AVAILABLE,
            std::memory_order_relaxed);

        if (m_listener) {
            m_listener->NodeConnected(nodeId, *m_connections[nodeId]);
        }
    }

    // check if the current node didn't figure out that the remote
    // died and was restarted (current node acting as receiver only).
    // this results in still owning old queue pair information
    // which cannot be re-used with the new remote
    if (m_connections[nodeId]->IsConnected() && lid != 0xFFFF &&
            m_connections[nodeId]->GetRemoteInfo().GetConManIdent() != ident) {
        // different connection manager though same node id
        // -> application restarted, kill old connection
        IBNET_LOG_DEBUG("Detected zombie connection to node 0x{:x}"
            " ({} != {}), killing...", nodeId,
            m_connections[nodeId]->GetRemoteInfo().GetConManIdent(), ident);

        m_jobThread.AddCloseJob(nodeId, true, false);
        m_jobThread.AddCreateJob(nodeId);
        return;
    }

    // send QP data to remote if connection established (remote might still
    // have to do that) or if we are still lacking the data
    uint32_t ownPhysicalQpIds[MAX_QPS_PER_CONNECTION];
    memset(ownPhysicalQpIds, 0xFFFFFFFF, sizeof(ownPhysicalQpIds));

    uint32_t cnt = 0;
    for (auto& it : m_connections[nodeId]->GetQps()) {
        ownPhysicalQpIds[cnt++] = it->GetPhysicalQpNum();
    }

    m_exchangeThread.SendExchgInfo(m_ownNodeId, m_connectionCtxIdent,
        m_device->GetLid(), ownPhysicalQpIds,
        remoteNodeInfo->GetAddress().GetAddress());
}

void IbConnectionManager::ConnectionContext::Close(uint16_t nodeId, bool force,
        bool shutdown)
{
    IBNET_LOG_INFO("Closing connection of 0x{:x}, force {}", nodeId, force);

    int32_t counter = m_connectionAvailable[nodeId].exchange(CONNECTION_CLOSING,
        std::memory_order_relaxed);

    if (!force) {
        // wait until remaining threads returned the connection
        while (true) {
            int32_t tmp = m_connectionAvailable[nodeId].load(
                std::memory_order_relaxed);

            if (CONNECTION_CLOSING - counter == tmp) {
                break;
            }

            std::this_thread::yield();
        }
    }

    // remove connection
    std::shared_ptr<IbConnection> connection = m_connections[nodeId];
    m_connections[nodeId] = nullptr;

    // check if someone else was faster and removed it already
    if (connection == nullptr) {
        return;
    }

    connection->Close(force);

    // re-use connection id
    m_availableConnectionIds.push_back(connection->GetConnectionId());

    connection.reset();

    m_openConnections--;

    m_connectionAvailable[nodeId].store(CONNECTION_NOT_AVAILABLE,
        std::memory_order_relaxed);

    m_discoveryContext.Invalidate(nodeId, shutdown);

    if (m_listener) {
        m_listener->NodeDisconnected(nodeId);
    }

    IBNET_LOG_DEBUG("Connection of 0x{:x}, force {} closed", nodeId, force);
}

IbConnectionManager::ExchangeThread::ExchangeThread(uint16_t ownNodeId,
        uint16_t socketPort, JobThread& jobThread) :
    ThreadLoop("IbConnectionManager-Exchange"),
    m_ownNodeId(ownNodeId),
    m_socket(std::move(std::make_unique<sys::SocketUDP>(socketPort))),
    m_jobThread(jobThread),
    m_noDataAvailable(false)
{

}

IbConnectionManager::ExchangeThread::~ExchangeThread(void)
{

}

void IbConnectionManager::ExchangeThread::SendDiscoveryReq(uint16_t ownNodeId,
        uint32_t destIp)
{
    Paket paket;

    paket.m_magic = PAKET_MAGIC;
    paket.m_type = PT_NODE_DISCOVERY_REQ;
    paket.m_nodeId = ownNodeId;

    ssize_t ret = m_socket->Send(&paket, sizeof(Paket), destIp,
        m_socket->GetPort());

    if (ret != sizeof(Paket)) {
        IBNET_LOG_ERROR("Sending discovery request to {} failed",
            sys::AddressIPV4(destIp));
    }
}

void IbConnectionManager::ExchangeThread::SendExchgInfo(uint16_t nodeId,
        uint32_t ident, uint16_t lid, uint32_t* physicalQpIds, uint32_t destIp)
{
    Paket paket;

    paket.m_magic = PAKET_MAGIC;
    paket.m_type = PT_NODE_CON_INFO;
    paket.m_nodeId = nodeId;
    paket.m_ident = ident;
    paket.m_lid = lid;
    memcpy(paket.m_physicalQpId, physicalQpIds, sizeof(paket.m_physicalQpId));

    ssize_t ret = m_socket->Send(&paket, sizeof(Paket), destIp,
        m_socket->GetPort());

    if (ret != sizeof(Paket)) {
        IBNET_LOG_ERROR("Sending exchg infoto {} failed",
            sys::AddressIPV4(destIp));
    }
}

void IbConnectionManager::ExchangeThread::_RunLoop(void)
{
    const size_t bufferSize = sizeof(Paket);
    uint32_t recvAddr = 0;

    ssize_t res = m_socket->Receive(&m_recvPaket, bufferSize, &recvAddr);

    if (res == bufferSize) {
        m_noDataAvailable = false;

        IBNET_LOG_TRACE(
            "Received paket from {}, magic 0x{:x}, type {}, nodeId 0x{:x}",
            sys::AddressIPV4(recvAddr), m_recvPaket.m_magic,
            m_recvPaket.m_type, m_recvPaket.m_nodeId);

        if (m_recvPaket.m_magic == PAKET_MAGIC) {
            switch (m_recvPaket.m_type) {
                case PT_NODE_DISCOVERY_REQ:
                    __SendDiscoveryResp(m_ownNodeId, recvAddr);
                    break;

                case PT_NODE_DISCOVERY_RESP:
                    m_jobThread.AddDiscoveredJob(m_recvPaket.m_nodeId,
                        recvAddr);
                    break;

                case PT_NODE_CON_INFO:
                    m_jobThread.AddCreateJob(m_recvPaket.m_nodeId,
                        m_recvPaket.m_ident,
                        m_recvPaket.m_lid,
                        m_recvPaket.m_physicalQpId);
                    break;

                default:
                    IBNET_LOG_ERROR("Unknown paket type {} from {}",
                        m_recvPaket.m_type, sys::AddressIPV4(recvAddr));
                return;
            }
        }
    } else {
        m_noDataAvailable = true;
    }

    if (m_noDataAvailable) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

bool IbConnectionManager::ExchangeThread::__SendDiscoveryResp(uint16_t ownNodeId,
                                                               uint32_t destIp)
{
    Paket paket;

    paket.m_magic = PAKET_MAGIC;
    paket.m_type = PT_NODE_DISCOVERY_RESP;
    paket.m_nodeId = ownNodeId;

    ssize_t ret = m_socket->Send(&paket, sizeof(Paket), destIp,
        m_socket->GetPort());

    return ret == sizeof(Paket);
}

IbConnectionManager::JobThread::JobThread(DiscoveryContext& discoveryContext,
        ConnectionContext& connectionContext) :
    ThreadLoop("IbConnectionManager-Job"),
    m_queue(1024),
    m_runDiscovery(true),
    m_discoveryContext(discoveryContext),
    m_connectionContext(connectionContext)
{

}

IbConnectionManager::JobThread::~JobThread(void)
{

}

void IbConnectionManager::JobThread::AddCreateJob(uint16_t nodeId)
{
    IbConnectionManagerJobQueue::Job job;
    job.m_type = IbConnectionManagerJobQueue::JT_CREATE;
    job.m_nodeId = nodeId;
    job.m_ident = 0xFFFFFFFF;
    job.m_lid = 0xFFFF;
    memset(job.m_physicalQpId, 0xFFFFFFFF, sizeof(job.m_physicalQpId));

    __AddJob(job);
}

void IbConnectionManager::JobThread::AddCreateJob(uint16_t nodeId,
        uint32_t ident, uint16_t lid, uint32_t* physicalQpIds)
{
    IbConnectionManagerJobQueue::Job job;
    job.m_type = IbConnectionManagerJobQueue::JT_CREATE;
    job.m_nodeId = nodeId;
    job.m_ident = ident;
    job.m_lid = lid;
    memcpy(job.m_physicalQpId, physicalQpIds, sizeof(job.m_physicalQpId));

    __AddJob(job);
}

void IbConnectionManager::JobThread::AddCloseJob(uint16_t nodeId, bool force,
        bool shutdown)
{
    IbConnectionManagerJobQueue::Job job;
    job.m_type = IbConnectionManagerJobQueue::JT_CLOSE;
    job.m_nodeId = nodeId;
    job.m_force = force;
    job.m_shutdown = shutdown;

    __AddJob(job);
}

void IbConnectionManager::JobThread::AddDiscoverJob(void)
{
    m_runDiscovery.store(true, std::memory_order_relaxed);
}

void IbConnectionManager::JobThread::AddDiscoveredJob(uint16_t nodeId,
        uint32_t remoteIpAddr)
{
    IbConnectionManagerJobQueue::Job job;
    job.m_type = IbConnectionManagerJobQueue::JT_DISCOVERED;
    job.m_nodeId = nodeId;
    job.m_ipAddr = remoteIpAddr;

    __AddJob(job);
}

bool IbConnectionManager::JobThread::IsQueueEmpty(void)
{
    return m_queue.IsEmpty();
}

void IbConnectionManager::JobThread::_RunLoop(void)
{
    // run discovery if no other jobs are available, prioritizing connection
    // creation and avoiding that discovery jobs are prioritized and cause
    // connection creation timeouts
    if (!m_queue.PopFront(m_job)) {
        bool val = true;

        if (m_runDiscovery.compare_exchange_strong(val, false,
                std::memory_order_relaxed)) {
            m_discoveryContext.Discover();
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    } else {
        IBNET_LOG_TRACE("Dispatching job type {}", m_job.m_type);

        switch (m_job.m_type) {
            case IbConnectionManagerJobQueue::JT_CREATE:
                m_connectionContext.Create(m_job.m_nodeId, m_job.m_ident , m_job.m_lid,
                    m_job.m_physicalQpId);
                break;

            case IbConnectionManagerJobQueue::JT_CLOSE:
                m_connectionContext.Close(m_job.m_nodeId, m_job.m_force,
                    m_job.m_shutdown);
                break;

            case IbConnectionManagerJobQueue::JT_DISCOVERED:
                m_discoveryContext.Discovered(m_job.m_nodeId, m_job.m_ipAddr);
                break;

            default:
                IBNET_LOG_ERROR("Cannot dispatch unknown job type {}",
                    m_job.m_type);
                break;
        }
    }
}

void IbConnectionManager::JobThread::__AddJob(
        IbConnectionManagerJobQueue::Job& job)
{
    IBNET_LOG_TRACE("Add job {}", job.m_type);

    while (!m_queue.PushBack(job)) {
        IBNET_LOG_WARN("Job queue full, waiting...");
        std::this_thread::sleep_for(
            std::chrono::milliseconds(1));
    }
}

}
}