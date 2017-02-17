package de.hhu.bsinfo.dxcompute.bench;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxcompute.DXComputeMessageTypes;
import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkIDRangeUtils;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by akguel on 14.02.17.
 */
public class NetworkBroadcastTask implements Task, NetworkHandler.MessageReceiver{
    private static final Logger LOGGER = LogManager.getFormatterLogger(NetworkEndToEndTask.class.getSimpleName());

    private volatile AtomicBoolean isFinished = new AtomicBoolean(false);
    private volatile AtomicLong m_receivedCnt = new AtomicLong(0);
    private int m_slaveCnt = 0;

    private boolean m_isFirstMessage = true;
    private long m_receiveTimeStart, m_receiveTimeEnd;

    @Expose
    private int m_messageCnt = 100;
    @Expose
    private int m_messageSize = 1000;
    @Expose
    private int m_threadCnt = 10;


    @Override
    public int execute(TaskContext p_ctx) {

        short[] slaveNodeIds = p_ctx.getCtxData().getSlaveNodeIds();
        m_slaveCnt = slaveNodeIds.length;
        short ownSlaveID = p_ctx.getCtxData().getSlaveId();

        if(slaveNodeIds.length <= 1) {
            System.out.println("The number of slave have to be at least two!");
            return -1;
        }

        NetworkService networkService = p_ctx.getDXRAMServiceAccessor().getService(NetworkService.class);
        networkService.registerReceiver(NetworkTestMessage.class, this);
        networkService.registerMessageType(DXComputeMessageTypes.BENCH_MESSAGE_TYPE, BenchMessages.NETWORK_TEST_MESSAGE, NetworkTestMessage.class);

        // get Messages per Thread and destination node id
        long[] messagesPerThread = ChunkIDRangeUtils.distributeChunkCountsToThreads(m_messageCnt, m_threadCnt);

        Thread[] threads = new Thread[m_threadCnt];
        long[] timeStart = new long[m_threadCnt];
        long[] timeEnd = new long[m_threadCnt];

        // pre create threads to use pooling
        NetworkTestMessage messages[] = new NetworkTestMessage[m_slaveCnt];
        for (int i = 0; i < m_slaveCnt; i++) {
            messages[i] = new NetworkTestMessage(slaveNodeIds[i], m_messageSize);
        }

        // thread runnables
        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            long messagesToSend = messagesPerThread[threadIdx];

            threads[i] = new Thread(() -> {

                timeStart[threadIdx] = System.nanoTime();

                for (int j = 0; j < messagesToSend; j++) {
                    // send message to every slave
                    for(int k = 0; k < m_slaveCnt; k++) {
                        if(k == ownSlaveID) continue;
                        try {
                            networkService.sendMessage(messages[k]);
                        } catch (NetworkException e) {
                            e.printStackTrace();
                        }
                    }
                }

                timeEnd[threadIdx] = System.nanoTime();
            });
        }

        for(Thread t : threads) {
            t.start();
        }

        boolean threadJoinFailed = false;
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (final InterruptedException e) {
                LOGGER.error("Joining thread failed", e);
                threadJoinFailed = true;
            }
        }

        if (threadJoinFailed) {
            return -2;
        }

        // Wait until all messages received
        //while(!isFinished.get());


        System.out.print("Times per thread:");
        for (int i = 0; i < m_threadCnt; i++) {
            System.out.printf("\nThread-%d: %f sec", i, (timeEnd[i] - timeStart[i]) / 1000.0 / 1000.0 / 1000.0);
        }
        System.out.println();

        // total time is measured by the slowest thread
        long totalTime = 0;
        for (int i = 0; i < m_threadCnt; i++) {
            long time = timeEnd[i] - timeStart[i];
            if (time > totalTime) {
                totalTime = time;
            }
        }

        System.out.printf("Total time: %f sec\n", totalTime / 1000.0 / 1000.0 / 1000.0);
        double sizeInMB = m_messageCnt*m_messageSize*(m_slaveCnt-1)/1000.0/1000.0;
        double timeInS = totalTime / 1000.0 / 1000.0 / 1000.0;
        double throughput = sizeInMB / timeInS;
        System.out.printf("Throughput Tx: %f MB/s\n", throughput);

        while(!isFinished.get());

        sizeInMB = m_messageCnt*m_messageSize*(m_slaveCnt-1)/1000.0/1000.0;
        timeInS = (m_receiveTimeEnd-m_receiveTimeStart) / 1000.0 / 1000.0 / 1000.0;
        System.out.printf("Throughput Rx: %f MB/s\n", sizeInMB/timeInS);

        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_messageCnt);
        p_exporter.writeInt(m_messageSize);
        p_exporter.writeInt(m_threadCnt);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_messageCnt = p_importer.readInt();
        m_messageSize = p_importer.readInt();
        m_threadCnt = p_importer.readInt();
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 3;
    }


    @Override
    public void onIncomingMessage(AbstractMessage p_message) {
        if(m_isFirstMessage){
            m_receiveTimeStart = System.nanoTime();
            m_isFirstMessage = false;
        }

        m_receivedCnt.incrementAndGet();

        if(m_receivedCnt.get() == (m_messageCnt * (m_slaveCnt-1))) {
            m_receiveTimeEnd = System.nanoTime();
            isFinished.compareAndSet(false, true);
        }

    }
}
