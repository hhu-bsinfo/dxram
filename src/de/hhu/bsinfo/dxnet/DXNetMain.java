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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxnet.core.messages.BenchmarkMessage;
import de.hhu.bsinfo.dxnet.core.messages.Messages;
import de.hhu.bsinfo.dxram.ms.tasks.PrintStatistics;
import de.hhu.bsinfo.dxram.util.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxram.util.TimeUnitGsonSerializer;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Execution: java -Dlog4j.configurationFile=config/log4j.xml -cp lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:dxram.jar de.hhu.bsinfo.dxnet.DXNetMain config/dxram.json Loopback 127.0.0.1
 */
public final class DXNetMain implements MessageReceiver {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXNetMain.class.getSimpleName());

    private static long ms_messageCount;
    private static int ms_messageSize;

    private long m_timeStart;

    private DXNetMain() {
        m_timeStart = System.nanoTime();
    }

    public static void main(final String[] p_arguments) {

        // Parse command line arguments
        if (p_arguments.length < 4) {
            System.out.println("Usage: config_file own_ip num_messages message_size num_threads");
            System.exit(-1);
        }
        String configPath = p_arguments[0];
        String ip = p_arguments[1];
        ms_messageCount = Long.parseLong(p_arguments[2]);
        ms_messageSize = Integer.parseInt(p_arguments[3]);
        int threads = Integer.parseInt(p_arguments[4]);

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
            InetSocketAddress socketAddress = new InetSocketAddress("255.255.255.255", 0xFFFF);
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
            //DXRAMJNIManager.loadJNIModule("JNIIbdxnet");
        } else if ("Loopback".equals(context.getCoreConfig().getDevice())) {
            LOGGER.debug("Loading loopback...");
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("Unknown device %s. Valid options: Ethernet, Infiniband or Loopback.", context.getCoreConfig().getDevice());
            // #endif /* LOGGER >= ERROR */
            System.exit(-1);
        }

        // Set own node ID and register all participating nodes
        NodeMap nodeMap = new DXNetNodeMap(context.getCoreConfig().getOwnNodeId());
        // ((DXNetNodeMap) nodeMap).addNode((short) 7, new InetSocketAddress("255.255.255.255", 0xFFFF));

        // Initialize DXNet
        DXNet dxnet = new DXNet(context.getCoreConfig(), context.getNIOConfig(), context.getIBConfig(), context.getLoopbackConfig(), nodeMap);

        // Register benchmark message in DXNet
        dxnet.registerMessageType(Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_MESSAGE, BenchmarkMessage.class);
        dxnet.register(Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_BENCHMARK_MESSAGE, new DXNetMain());

        // Workload
        Runnable task = () -> {
            BenchmarkMessage message = new BenchmarkMessage((short) 7, ms_messageSize);
            for (int i = 0; i < ms_messageCount / threads; i++) {
                try {
                    dxnet.sendMessage(message);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }
        };

        long timeStart = System.nanoTime();
        LOGGER.info("Starting workload...");
        Thread[] threadArray = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            threadArray[i] = new Thread(task);
            threadArray[i].start();
        }

        for (int i = 0; i < threads; i++) {
            try {
                threadArray[i].join();
            } catch (InterruptedException ignore) {
            }
        }
        LOGGER.info("Workload finished on sender.");

        /*PrintStatistics.printStatisticsToOutput(System.out);

        long timeDiff = System.nanoTime() - timeStart;
        LOGGER.info("Runtime: %d ms", timeDiff / 1000 / 1000);
        LOGGER.info("Time per message: %d ns", timeDiff / ms_messageCount);
        LOGGER.info("Throughput: %d MB/s", (int) ((double) ms_messageCount * ms_messageSize / 1024 / 1024 / ((double) timeDiff / 1000 / 1000 / 1000)));
        LOGGER.info("Throughput (with overhead): %d MB/s",
                (int) ((double) ms_messageCount * (ms_messageSize + ObjectSizeUtil.sizeofCompactedNumber(ms_messageSize) + 10) / 1024 / 1024 /
                        ((double) timeDiff / 1000 / 1000 / 1000)));
        System.exit(0);*/
    }

    private AtomicLong m_messages = new AtomicLong(0);

    @Override
    public void onIncomingMessage(Message p_message) {
        if (m_messages.incrementAndGet() == ms_messageCount) {
            LOGGER.info("Workload finished on receiver.");
            PrintStatistics.printStatisticsToOutput(System.out);

            long timeDiff = System.nanoTime() - m_timeStart;
            LOGGER.info("Runtime: %d ms", timeDiff / 1000 / 1000);
            LOGGER.info("Time per message: %d ns", timeDiff / ms_messageCount);
            LOGGER.info("Throughput: %d MB/s", (int) ((double) ms_messageCount * ms_messageSize / 1024 / 1024 / ((double) timeDiff / 1000 / 1000 / 1000)));
            LOGGER.info("Throughput (with overhead): %d MB/s",
                    (int) ((double) ms_messageCount * (ms_messageSize + ObjectSizeUtil.sizeofCompactedNumber(ms_messageSize) + 10) / 1024 / 1024 /
                            ((double) timeDiff / 1000 / 1000 / 1000)));
            System.exit(0);
        }
    }
}
