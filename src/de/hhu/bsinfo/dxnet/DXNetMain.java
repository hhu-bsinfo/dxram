/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxnet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxnet.core.messages.BenchmarkMessage;
import de.hhu.bsinfo.dxnet.core.messages.BenchmarkRequest;
import de.hhu.bsinfo.dxnet.core.messages.BenchmarkResponse;
import de.hhu.bsinfo.dxnet.core.messages.Messages;
import de.hhu.bsinfo.dxutils.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.TimeUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.UnsafeHandler;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import de.hhu.bsinfo.dxutils.stats.ExportStatistics;
import de.hhu.bsinfo.dxutils.stats.PrintStatistics;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Execution: java -Dlog4j.configurationFile=config/log4j.xml -cp lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:dxram.jar de.hhu.bsinfo.dxnet.DXNetMain config/dxram.json Loopback 127.0.0.1
 */
public final class DXNetMain implements MessageReceiver {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXNetMain.class.getSimpleName());

    private static final boolean POOLING = true;
    private static final boolean EXPORT_STATISTICS = true;

    private static DXNet ms_dxnet;

    private static boolean ms_isServer = false;
    private static long ms_messageCount;
    private static int ms_messageSize;
    private static BenchmarkResponse[] ms_responses;

    private static boolean ms_serverStart = false;
    private static volatile boolean ms_remoteFinished = false;

    private long m_timeStart;

    private DXNetMain() {
        m_timeStart = System.nanoTime();
    }

    public static void main(final String[] p_arguments) {

        // Parse command line arguments
        if (p_arguments.length < 7) {
            System.out.println("Usage: config_file mode own_ip role num_messages message_size num_threads [server_ip|client_ip]");
            System.exit(-1);
        }
        String configPath = p_arguments[0];
        String mode = p_arguments[1].toLowerCase();
        if (!"messages".equals(mode) && !"requests".equals(mode)) {
            System.out.println("Mode must be Messages or Requests (case insensitive).");
            System.exit(-1);
        }
        String ownIP = p_arguments[2];
        String role = p_arguments[3].toLowerCase();
        if (!"server".equals(role) && !"client".equals(role)) {
            System.out.println("Role must be Server or Client (case insensitive)." +
                    " Start one server first and then one client (for Loopback starting one client (without a server) is enough).");
            System.exit(-1);
        }
        if (p_arguments.length == 7) {
            if ("server".equals(role)) {
                System.out.println("When starting the server the client_ip must be set (Loopback: 0 or any other number).");
            } else {
                System.out.println("When starting the client the server_ip must be set and the server must be running (Loopback: 0 or any other number).");
            }
            System.exit(-1);
        }
        ms_messageCount = Long.parseLong(p_arguments[4]);
        ms_messageSize = Integer.parseInt(p_arguments[5]);
        int threads = Integer.parseInt(p_arguments[6]);

        // Load configuration file for network related parameters
        LOGGER.info("Loading configuration '%s'...", configPath);
        DXNetContext context = new DXNetContext();
        File file = new File(configPath);
        Gson gson = new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation()
                .registerTypeAdapter(StorageUnit.class, new StorageUnitGsonSerializer()).registerTypeAdapter(TimeUnit.class, new TimeUnitGsonSerializer())
                .create();
        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    LOGGER.error("Creating new config file %s failed", file);
                    System.exit(-1);
                }
            } catch (final IOException e) {
                LOGGER.error("Creating new config file %s failed: %s", file, e.getMessage());
                System.exit(-1);
            }

            String jsonString = gson.toJson(context);
            try {
                PrintWriter writer = new PrintWriter(file);
                writer.print(jsonString);
                writer.close();
            } catch (final FileNotFoundException e) {
                // we can ignored this here, already checked that
            }
        }

        JsonElement element = null;
        try {
            element = gson.fromJson(new String(Files.readAllBytes(Paths.get(configPath))), JsonElement.class);
        } catch (final Exception e) {
            LOGGER.error("Could not load configuration '%s': %s", configPath, e.getMessage());
            System.exit(-1);
        }

        if (element == null) {
            LOGGER.error("Could not load configuration '%s': empty configuration file", configPath);
            System.exit(-1);
        }

        try {
            context = gson.fromJson(element, DXNetContext.class);
        } catch (final Exception e) {
            LOGGER.error("Loading configuration '%s' failed: %s", configPath, e.getMessage());
            System.exit(-1);
        }

        if (context == null) {
            LOGGER.error("Loading configuration '%s' failed: context null", configPath);
            System.exit(-1);
        }

        // Verify configuration values
        if (!context.verify()) {
            System.exit(-1);
        }

        if ("Ethernet".equals(context.getCoreConfig().getDevice())) {
            LOGGER.debug("Loading ethernet...");

            // Check if given ip address is bound to one of this node's network interfaces
            boolean found = false;
            InetSocketAddress socketAddress = new InetSocketAddress(ownIP, 0xFFFF);
            InetAddress myAddress = socketAddress.getAddress();
            try {
                Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                outerloop:
                while (networkInterfaces.hasMoreElements()) {
                    NetworkInterface currentNetworkInterface = networkInterfaces.nextElement();
                    Enumeration<InetAddress> addresses = currentNetworkInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        InetAddress currentAddress = addresses.nextElement();
                        if (myAddress.equals(currentAddress)) {
                            System.out.printf("%s is bound to %s\n", myAddress.getHostAddress(), currentNetworkInterface.getDisplayName());
                            found = true;
                            break outerloop;
                        }
                    }
                }
            } catch (final SocketException e1) {
                System.out.printf("Could not get network interfaces for ip confirmation\n");
            } finally {
                if (!found) {
                    System.out.printf("Could not find network interface with address %s\n", myAddress.getHostAddress());
                    System.exit(-1);
                }
            }
        } else if ("Infiniband".equals(context.getCoreConfig().getDevice())) {
            LOGGER.debug("Loading infiniband...");

            System.load(System.getProperty("user.dir") + "/jni/libJNIIbdxnet.so");
        } else if ("Loopback".equals(context.getCoreConfig().getDevice())) {
            if ("server".equals(role)) {
                System.out.println("Server role is not allowed for loopback device");
                System.exit(-1);
            }

            LOGGER.debug("Loading loopback...");
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("Unknown device %s. Valid options: Ethernet, Infiniband or Loopback.", context.getCoreConfig().getDevice());
            // #endif /* LOGGER >= ERROR */
            System.exit(-1);
        }

        // Set own node ID and register all participating nodes
        short destinationNID;
        NodeMap nodeMap;
        if ("Loopback".equals(context.getCoreConfig().getDevice())) {
            // With loopback device one instance is started, only
            context.getCoreConfig().setOwnNodeId((short) 6);
            nodeMap = new DXNetNodeMap(context.getCoreConfig().getOwnNodeId());
            destinationNID = (short) 7;
            ms_isServer = false;
        } else {
            // Server has NodeID 7 (and port 22222 when using Ethernet), client NodeID 6 (and port 22223 when using Ethernet)
            if ("server".equals(role)) {
                context.getCoreConfig().setOwnNodeId((short) 7);
                nodeMap = new DXNetNodeMap(context.getCoreConfig().getOwnNodeId());
                ((DXNetNodeMap) nodeMap).addNode((short) 7, new InetSocketAddress(ownIP, 22222));
                ((DXNetNodeMap) nodeMap).addNode((short) 6, new InetSocketAddress(p_arguments[7], 22223));
                destinationNID = (short) 6;
                ms_isServer = true;
            } else {
                context.getCoreConfig().setOwnNodeId((short) 6);
                nodeMap = new DXNetNodeMap(context.getCoreConfig().getOwnNodeId());
                ((DXNetNodeMap) nodeMap).addNode((short) 6, new InetSocketAddress(ownIP, 22223));
                ((DXNetNodeMap) nodeMap).addNode((short) 7, new InetSocketAddress(p_arguments[7], 22222));
                destinationNID = (short) 7;
                ms_isServer = false;
            }
        }

        // Initialize DXNet
        ms_dxnet = new DXNet(context.getCoreConfig(), context.getNIOConfig(), context.getIBConfig(), context.getLoopbackConfig(), nodeMap);

        // Register benchmark message in DXNet
        ms_dxnet.registerMessageType(Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_MESSAGE, BenchmarkMessage.class);
        ms_dxnet.registerMessageType(Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_REQUEST, BenchmarkRequest.class);
        ms_dxnet.registerMessageType(Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_RESPONSE, BenchmarkResponse.class);
        ms_dxnet.register(Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_MESSAGE, new DXNetMain());
        ms_dxnet.register(Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_REQUEST, new DXNetMain());
        ms_dxnet.register(Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_RESPONSE, new DXNetMain());

        // Workload
        Runnable task;
        if ("messages".equals(mode)) {
            if (POOLING) {
                task = () -> {
                    long messageCount = ms_messageCount / threads;
                    if (Integer.parseInt(Thread.currentThread().getName()) == threads - 1) {
                        messageCount += ms_messageCount % threads;
                    }

                    BenchmarkMessage message = new BenchmarkMessage(destinationNID, ms_messageSize);
                    for (int i = 0; i < messageCount; i++) {
                        try {
                            ms_dxnet.sendMessage(message);
                        } catch (NetworkException e) {
                            e.printStackTrace();
                        }
                    }
                };
            } else {
                task = () -> {
                    long messageCount = ms_messageCount / threads;
                    if (Integer.parseInt(Thread.currentThread().getName()) == threads - 1) {
                        messageCount += ms_messageCount % threads;
                    }

                    for (int i = 0; i < messageCount; i++) {
                        try {
                            BenchmarkMessage message = new BenchmarkMessage(destinationNID, ms_messageSize);
                            ms_dxnet.sendMessage(message);
                        } catch (NetworkException e) {
                            e.printStackTrace();
                        }
                    }
                };
            }
        } else {
            if (POOLING) {
                int numberOfMessageHandler = context.getCoreConfig().getNumMessageHandlerThreads();
                ms_responses = new BenchmarkResponse[numberOfMessageHandler];
                for (int i = 0; i < numberOfMessageHandler; i++) {
                    ms_responses[i] = new BenchmarkResponse();
                }

                task = () -> {
                    long messageCount = ms_messageCount / threads;
                    if (Integer.parseInt(Thread.currentThread().getName()) == threads - 1) {
                        messageCount += ms_messageCount % threads;
                    }

                    BenchmarkRequest request = new BenchmarkRequest(destinationNID, ms_messageSize);
                    for (int i = 0; i < messageCount; i++) {
                        try {
                            request.reuse();
                            ms_dxnet.sendSync(request, -1, true);
                        } catch (NetworkException e) {
                            e.printStackTrace();
                        }
                    }
                };
            } else {
                task = () -> {
                    long messageCount = ms_messageCount / threads;
                    if (Integer.parseInt(Thread.currentThread().getName()) == threads - 1) {
                        messageCount += ms_messageCount % threads;
                    }

                    for (int i = 0; i < messageCount; i++) {
                        try {
                            BenchmarkRequest request = new BenchmarkRequest(destinationNID, ms_messageSize);
                            ms_dxnet.sendSync(request, -1, true);
                        } catch (NetworkException e) {
                            e.printStackTrace();
                        }
                    }
                };
            }
        }

        if ("server".equals(role)) {
            // Server answers requests, only -> let main thread wait
            while (!ms_serverStart) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                UnsafeHandler.getInstance().getUnsafe().loadFence();
            }
        }

        long timeStart = System.nanoTime();
        LOGGER.info("Starting workload...");
        Thread[] threadArray = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            threadArray[i] = new Thread(task);
            threadArray[i].setName(String.valueOf(i));
            threadArray[i].start();
        }

        for (int i = 0; i < threads; i++) {
            try {
                threadArray[i].join();
            } catch (InterruptedException ignore) {
            }
        }
        LOGGER.info("Workload finished on sender.");

        if ("requests".equals(mode)) {
            // Printing statistics
            while (!ms_dxnet.isRequestMapEmpty()) {
                LockSupport.parkNanos(100);
            }

            if (EXPORT_STATISTICS) {
                ExportStatistics.writeStatisticsToFile(System.getProperty("user.dir") + "/stats/DXNetMain/" + role + '/');
            } else {
                PrintStatistics.printStatisticsToOutput(System.out);
            }

            long timeDiff = System.nanoTime() - timeStart;
            LOGGER.info("Runtime: %d ms", timeDiff / 1000 / 1000);
            LOGGER.info("Time per message: %d ns", timeDiff / ms_messageCount);
            LOGGER.info("Throughput: %f MB/s", (double) ms_messageCount * ms_messageSize / 1024 / 1024 / ((double) timeDiff / 1000 / 1000 / 1000));
            LOGGER.info("Throughput (with overhead): %f MB/s",
                    (double) ms_messageCount * (ms_messageSize + ObjectSizeUtil.sizeofCompactedNumber(ms_messageSize) + 10) / 1024 / 1024 /
                            ((double) timeDiff / 1000 / 1000 / 1000));
        }

        while (!ms_remoteFinished) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    private AtomicLong m_messages = new AtomicLong(0);

    @Override
    public void onIncomingMessage(Message p_message) {
        short destinationNID;

        if (ms_isServer) {
            if (!ms_serverStart) {
                ms_serverStart = true;
                UnsafeHandler.getInstance().getUnsafe().storeFence();
            }
            destinationNID = (short) 6;
        } else {
            destinationNID = (short) 7;
        }

        if (p_message.getSubtype() == Messages.SUBTYPE_BENCHMARK_REQUEST) {
            BenchmarkResponse response;
            if (POOLING) {
                response = ms_responses[Integer.parseInt(Thread.currentThread().getName().substring(24)) - 1];
                response.reuse((BenchmarkRequest) p_message, Messages.SUBTYPE_BENCHMARK_RESPONSE);
            } else {
                response = new BenchmarkResponse((BenchmarkRequest) p_message);
            }
            response.setDestination(destinationNID);

            try {
                ms_dxnet.sendMessage(response);
            } catch (NetworkException e) {
                e.printStackTrace();
            }

            if (m_messages.incrementAndGet() == ms_messageCount) {
                ms_remoteFinished = true;
            }
        } else {
            if (m_messages.incrementAndGet() == ms_messageCount) {
                LOGGER.info("Workload finished on receiver.");

                if (EXPORT_STATISTICS) {
                    ExportStatistics.writeStatisticsToFile(System.getProperty("user.dir") + "/stats/DXNetMain/client/");
                } else {
                    PrintStatistics.printStatisticsToOutput(System.out);
                }

                long timeDiff = System.nanoTime() - m_timeStart;
                LOGGER.info("Runtime: %d ms", timeDiff / 1000 / 1000);
                LOGGER.info("Time per message: %d ns", timeDiff / ms_messageCount);
                LOGGER.info("Throughput: %f MB/s", (double) ms_messageCount * ms_messageSize / 1024 / 1024 / ((double) timeDiff / 1000 / 1000 / 1000));
                LOGGER.info("Throughput (with overhead): %f MB/s",
                        (double) ms_messageCount * (ms_messageSize + ObjectSizeUtil.sizeofCompactedNumber(ms_messageSize) + 10) / 1024 / 1024 /
                                ((double) timeDiff / 1000 / 1000 / 1000));

                ms_remoteFinished = true;
            }
        }
    }
}
