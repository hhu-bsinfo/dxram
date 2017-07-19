package de.hhu.bsinfo.net.ib;

/**
 * Created by nothaas on 6/20/17.
 */
public class TestJNIIbnet /* implements JNIIbdxnet.SendHandler, JNIIbdxnet.RecvHandler, JNIIbdxnet.DiscoveryHandler, JNIIbdxnet.ConnectionHandler */ {

    //    private short m_nodeId = -1;
    //    private int m_maxRecvReqs = 10;
    //    private int m_maxSendReqs = 10;
    //    private int m_inOutBufferSize = 4096;
    //    private int m_flowControlMaxRecvReqs = 10;
    //    private int m_flowControlMaxSendReqs = 10;
    //    private int m_outgoingJobPoolSize = 10000;
    //    private int m_sendThreads = 1;
    //    private int m_recvThreads = 1;
    //    private int m_maxNumConnections = 100;
    //
    //    private short m_remoteNodeId = -1;
    //    private volatile boolean m_run = false;
    //
    //    private IBWriteInterestManager m_writeInterestManager;
    //    private IBRecvBufferPool m_bufferPool;
    //
    //    private BandwidthThread m_bandwidth;
    //
    //    private AtomicInteger m_flowControlUnconfirmed = new AtomicInteger(0);
    //
    //    public static void main(final String[] p_args) {
    //        TestJNIIbnet test = new TestJNIIbnet();
    //        test.run(p_args);
    //    }
    //
    //    public TestJNIIbnet() {
    //
    //    }
    //
    //    public void run(final String[] p_args) {
    //        if (p_args.length < 3) {
    //            System.out.println("Usage: TestJNIIbnet <full path libJNIIbnet.so> <own nid> <remote nid> <hostnames nodes> ...");
    //            return;
    //        }
    //
    //        System.load(p_args[0]);
    //        m_nodeId = (short) Integer.parseInt(p_args[1]);
    //        m_remoteNodeId = (short) Integer.parseInt(p_args[2]);
    //
    //        m_writeInterestManager = new IBWriteInterestManager();
    //        m_bufferPool = new IBRecvBufferPool(m_inOutBufferSize, 100);
    //
    //        m_bandwidth = new BandwidthThread();
    //        m_bandwidth.start();
    //
    //        if (!JNIIbdxnet
    //                .init(m_nodeId, m_maxRecvReqs, m_maxSendReqs, m_inOutBufferSize, m_flowControlMaxRecvReqs, m_flowControlMaxSendReqs, m_outgoingJobPoolSize,
    //                m_sendThreads, m_recvThreads, m_maxNumConnections, this, this, this, this, false, true)) {
    //            System.out.println("Initializing JNIIbdxnet failed");
    //            return;
    //        }
    //
    //        for (int i = 3; i < p_args.length; i++) {
    //            byte[] bytes = new byte[0];
    //            try {
    //                bytes = InetAddress.getByName(p_args[i]).getAddress();
    //            } catch (UnknownHostException e) {
    //                e.printStackTrace();
    //            }
    //            int val = (int) (((long) bytes[0] & 0xFF) << 24 | bytes[1] & 0xFF << 16 | bytes[2] & 0xFF << 8 | bytes[3] & 0xFF);
    //
    //            JNIIbdxnet.addNode(val);
    //        }
    //
    //        // FIXME wait a moment for the subsystem
    //        try {
    //            Thread.sleep(2000);
    //        } catch (InterruptedException e) {
    //            e.printStackTrace();
    //        }
    //
    //        while (!m_run) {
    //            Thread.yield();
    //        }
    //
    //        ByteBuffer buffer = ByteBuffer.allocateDirect(m_inOutBufferSize);
    //
    //        for (int i = 0; i < m_inOutBufferSize; i++) {
    //            buffer.put((byte) i);
    //        }
    //
    //        buffer.rewind();
    //
    //        while (true) {
    //            m_bandwidth.enterSend();
    //            // TODO make flow control window setable
    //            while (m_flowControlUnconfirmed.get() > 1024 * 1024) {
    //                Thread.yield();
    //            }
    //
    //            // TODO use new BufferQueue to post data to
    //
    //            m_writeInterestManager.pushBackDataInterest(m_remoteNodeId, buffer.capacity());
    //
    //            m_bandwidth.addSend(buffer.capacity());
    //
    //            m_flowControlUnconfirmed.addAndGet(buffer.capacity());
    //
    //            m_bandwidth.exitSend();
    //        }
    //    }
    //
    //    @Override
    //    public void nodeDiscovered(short p_nodeId) {
    //        System.out.printf("nodeDiscovered %X\n", p_nodeId);
    //
    //        if (p_nodeId != m_nodeId) {
    //            m_run = true;
    //        }
    //    }
    //
    //    @Override
    //    public void nodeInvalidated(short p_nodeId) {
    //        System.out.printf("nodeInvalidated %X\n", p_nodeId);
    //    }
    //
    //    @Override
    //    public void nodeConnected(short p_nodeId) {
    //        System.out.printf("nodeConnected %X\n", p_nodeId);
    //    }
    //
    //    @Override
    //    public void nodeDisconnected(short p_nodeId) {
    //        System.out.printf("nodeDisconnected %X\n", p_nodeId);
    //    }
    //
    //    @Override
    //    public void getNextDataToSend(short p_prevNodeIdWritten, int p_prevDataWrittenLen, long p_prevFlowControlWritten) {
    //
    //    }
    //
    //    @Override
    //    public ByteBuffer getReceiveBuffer(int p_size) {
    //        ByteBuffer buffer = m_bufferPool.getBuffer();
    //
    //        if (buffer == null) {
    //            System.out.println("ERROR out of buffers");
    //        }
    //
    //        return buffer;
    //    }
    //
    //    @Override
    //    public void receivedBuffer(short p_sourceNodeId, ByteBuffer p_buffer, int p_length) {
    //        //System.out.printf("Buffer received: %X, %d\n", p_sourceNodeId, p_length);
    //
    //        m_bandwidth.addRecv(p_length);
    //
    //        m_writeInterestManager.pushBackFlowControlInterest(p_sourceNodeId, p_length);
    //
    //        m_bufferPool.returnBuffer(p_buffer);
    //    }
    //
    //    @Override
    //    public void receivedFlowControlData(short p_sourceNodeId, int p_bytes) {
    //        //System.out.printf("Received flow control: %X %d\n", p_sourceNodeId, p_bytes);
    //
    //        if (p_sourceNodeId == m_remoteNodeId) {
    //            m_flowControlUnconfirmed.addAndGet(-p_bytes);
    //        } else {
    //            System.out.printf("ERROR received unexpected flow control data from node 0x%X\n", p_sourceNodeId);
    //        }
    //    }
    //
    //    private class BandwidthThread extends Thread {
    //        private AtomicLong m_bytesSend = new AtomicLong();
    //        private AtomicLong m_bytesRecv = new AtomicLong();
    //
    //        private long m_sendTimeTmp;
    //        private AtomicLong m_sendTime = new AtomicLong();
    //        private AtomicLong m_sendCount = new AtomicLong();
    //
    //        public void addSend(final long p_bytes) {
    //            m_bytesSend.addAndGet(p_bytes);
    //        }
    //
    //        public void addRecv(final long p_bytes) {
    //            m_bytesRecv.addAndGet(p_bytes);
    //        }
    //
    //        public void enterSend() {
    //            m_sendTimeTmp = System.nanoTime();
    //        }
    //
    //        public void exitSend() {
    //            m_sendTime.addAndGet(System.nanoTime() - m_sendTimeTmp);
    //            m_sendCount.incrementAndGet();
    //        }
    //
    //        @Override
    //        public void run() {
    //            long sendPrev = 0;
    //            long recvPrev = 0;
    //
    //            while (true) {
    //                try {
    //                    Thread.sleep(1000);
    //                } catch (InterruptedException e) {
    //                    e.printStackTrace();
    //                }
    //
    //                long sendCur = m_bytesSend.get();
    //                long recvCur = m_bytesRecv.get();
    //
    //                System.out.printf("Throughput send: %f mb/sec\n", (sendCur - sendPrev) / 1024.0 / 1024.0);
    //                System.out.printf("Throughput recv: %f mb/sec\n", (recvCur - recvPrev) / 1024.0 / 1024.0);
    //                System.out.printf("Send time %f us\n", m_sendTime.get() / 1000.0 / m_sendCount.get());
    //
    //                Runtime runtime = Runtime.getRuntime();
    //
    //                System.out.printf("Runtime used memory: %f\n", (runtime.totalMemory() - runtime.freeMemory()) / 1024.0 / 1024.0);
    //
    //                sendPrev = sendCur;
    //                recvPrev = recvCur;
    //            }
    //        }
    //    }
}
