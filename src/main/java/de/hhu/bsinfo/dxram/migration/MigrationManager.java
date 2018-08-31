/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.migration;


import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkMigrationComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.migration.data.MigrationPayload;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;
import de.hhu.bsinfo.dxram.migration.messages.MigrationFinish;
import de.hhu.bsinfo.dxram.migration.messages.MigrationMessages;
import de.hhu.bsinfo.dxram.migration.messages.MigrationPush;
import de.hhu.bsinfo.dxram.migration.progress.MigrationProgress;
import de.hhu.bsinfo.dxram.migration.progress.MigrationProgressTracker;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.NodeID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class MigrationManager implements MessageReceiver, ChunkMigrator {

    public static final ThreadFactory THREAD_FACTORY = new MigrationThreadFactory();

    private static final Logger log = LoggerFactory.getLogger(MigrationManager.class);

    private static final AtomicLong MIGRATION_COUNTER = new AtomicLong(0);

    private final ExecutorService m_executor;

    private final AbstractBootComponent m_boot;
    private final BackupComponent m_backup;
    private final ChunkMigrationComponent m_chunk;
    private final MemoryManagerComponent m_memoryManager;
    private final NetworkComponent m_network;

    private final int m_workerCount;

    private final MigrationProgressTracker m_progressTracker = new MigrationProgressTracker();

    public MigrationManager(int p_workerCount, final DXRAMComponentAccessor p_componentAccessor) {
        m_workerCount = p_workerCount;
        m_executor = Executors.newFixedThreadPool(p_workerCount, THREAD_FACTORY);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkMigrationComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    /**
     * Migrates the specified chunk range to the target node using multiple worker threads.
     *
     * @param p_target The target node.
     * @param p_range The chunk range.
     * @return A ticket containing information associated with the created migration.
     */
    public MigrationTicket<MigrationStatus> migrateRange(final short p_target, final LongRange p_range) {
        MigrationIdentifier identifier = new MigrationIdentifier(m_boot.getNodeId(), p_target);
        List<MigrationTask> tasks = createMigrationTasks(identifier, p_range);

        CompletableFuture<MigrationStatus> future = m_progressTracker.register(identifier, tasks.stream()
                .flatMap(task -> task.getRanges().stream()).collect(Collectors.toList()));

        tasks.forEach(m_executor::execute);

        return new MigrationTicket<>(future, identifier);
    }

//    public CompletableFuture<Void> migrateRanges(final short p_target, final List<LongRange> p_ranges) {
//        MigrationIdentifier rootIdentifier;
//        MigrationIdentifier taskIdentifier;
//        MigrationTask task;
//        MigrationProgress progress;
//        short source = m_boot.getNodeId();
//
//        for (LongRange range : p_ranges) {
//            rootIdentifier = new MigrationIdentifier(source, p_target, range.getFrom(), range.getTo());
//            task = new MigrationTask(this, new MigrationIdentifier(source, p_target, range.getFrom(), range.getTo(), 0), range.getFrom(), range.getTo());
//            progress = new MigrationProgress(1);
//            m_progressMap.put(rootIdentifier, progress);
//        }
//
//        @SuppressWarnings("unchecked")
//        CompletableFuture<MigrationStatus>[] futureList = (CompletableFuture<MigrationStatus>[]) IntStream.range(0, p_ranges.length - 1)
//                .mapToObj(i -> migrateRange(p_target, p_ranges[i], p_ranges[i + 1]))
//                .toArray();
//
//        return CompletableFuture.allOf(futureList);
//    }

    /**
     * Creates multiple migration tasks using the specified migration identifier.
     *
     * @param p_identifier The migration identifier.
     * @return An array containing migration tasks.
     */
    public List<MigrationTask> createMigrationTasks(MigrationIdentifier p_identifier, LongRange p_range) {
        List<MigrationTask> tasks = new ArrayList<>(m_workerCount);

        long[] partitions = partition(p_range.getFrom(), p_range.getTo(), m_workerCount);

        log.debug("{} {}", String.format("[%X,%X]", partitions[0], partitions[1]), p_range);

        List<LongRange> chunkRange;
        for (int i = 0, j = 0; i < partitions.length - 1; i += 2, j++) {
            chunkRange = Collections.singletonList(new LongRange(partitions[i], partitions[i + 1]));
            tasks.add(new MigrationTask(this, p_identifier, chunkRange));
        }

        return tasks;
    }

    // TODO(krakowski)
    //  Move this method to dxutils
    public static long[] partition(long p_start, long p_end, int p_count) {
        int elementCount = (int) (p_end - p_start);

        if (p_count > elementCount) {
            throw new IllegalArgumentException("Insufficient number of elements for " + p_count + " partitions");
        }

        long[] result = new long[p_count * 2];

        int length = (elementCount / p_count) + 1;
        long split = p_start;

        for (int i = 0; i < elementCount % p_count; i++) {
            result[i * 2] = length * i + p_start;
            result[(i * 2) + 1] = result[i * 2] + length;
            split = result[(i * 2) + 1];
        }

        length = elementCount / p_count;
        p_start = split;

        for (int i = elementCount % p_count; i < p_count; i++) {
            result[i * 2] = length * (i - (elementCount % p_count)) + p_start;
            result[(i * 2) + 1] = result[i * 2] + length;
        }

        return result;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message == null) {
            log.warn("Received null message");
            return;
        }

        if (p_message.getType() != DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE) {
            log.warn("Received wrong message type {}", p_message.getType());
        }

        m_executor.execute(() -> {
            switch (p_message.getSubtype()) {
                case MigrationMessages.SUBTYPE_MIGRATION_PUSH:
                    handle((MigrationPush) p_message);
                    break;
                case MigrationMessages.SUBTYPE_MIGRATION_FINISH:
                    handle((MigrationFinish) p_message);
                    break;
            }
        });
    }

    @Override
    public void onStatus(MigrationIdentifier p_identifier, long p_startId, long p_endId, Status p_result) {
        log.debug("Received Result {} for chunk range [{}, {}]", p_result, ChunkID.toHexString(p_startId),
                ChunkID.toHexString(p_endId));
    }

    @Override
    public Status migrate(MigrationIdentifier p_identifier, List<LongRange> p_ranges) {
        int chunkCount = LongRange.collectionToSize(p_ranges);

        if (m_boot.getNodeId() == p_identifier.getTarget()) {
            log.error("The migration target has to be another node");
            return Status.INVALID_ARG;
        }

        byte[][] data = new byte[chunkCount][];

        log.debug("Collecting {} chunks from memory", chunkCount);

        m_memoryManager.lockAccess();

        int index = 0;
        for (LongRange range : p_ranges) {
            for (long chunkId = range.getFrom(); chunkId < range.getTo(); chunkId++) {

                if ((data[index] = m_memoryManager.get(chunkId)) == null) {
                    log.warn("Chunk {} does not exist", ChunkID.toHexString(chunkId));
                    m_memoryManager.unlockAccess();
                    throw new IllegalArgumentException("Can't migrate non-existent chunks");
                }

                index++;
            }
        }

        m_memoryManager.unlockAccess();

        log.debug("Creating chunk ranges {} and migration push message for node {}",
                LongRange.collectionToString(p_ranges),
                NodeID.toHexString(p_identifier.getTarget()));

        MigrationPayload migrationPayload = new MigrationPayload(p_ranges, data);

        MigrationPush migrationPush = new MigrationPush(p_identifier, migrationPayload);

        int size = Arrays.stream(data)
                .map(a -> a.length)
                .reduce(0, (a, b) -> a + b);

        try {
            log.debug("Sending chunk ranges {} to {} containing {}",
                    LongRange.collectionToString(p_ranges),
                    NodeID.toHexString(migrationPush.getDestination()),
                    readableFileSize(size));

            m_network.sendMessage(migrationPush);
        } catch (NetworkException e) {
            log.error("Couldn't send migration push to target", e);
            return Status.NOT_SENT;
        }

        return Status.SENT;
    }

    public static String readableFileSize(long size) {
        if (size <= 0){
            return "0";
        }

        final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return new DecimalFormat("###0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    private void handle(final MigrationPush p_migrationPush) {
        final MigrationPayload payload = p_migrationPush.getPayload();
        int size = payload.getSize();

        List<LongRange> ranges = payload.getLongRanges();

        log.debug("Received chunk range {} from {} containing {}",
                LongRange.collectionToString(ranges),
                NodeID.toHexString(p_migrationPush.getDestination()),
                readableFileSize(size));

        final long[] chunkIds = ranges.stream().flatMapToLong(range -> Arrays.stream(range.toArray())).toArray();

        log.debug("Saving received chunks");

        boolean status = m_chunk.putMigratedChunks(chunkIds, payload.getData());

        log.debug("Storing migrated chunks {}", status ? "succeeded" : "failed");

        final MigrationFinish migrationFinish = new MigrationFinish(p_migrationPush.getIdentifier(), ranges, status);

        try {
            log.debug("Sending response to {}", NodeID.toHexString(migrationFinish.getDestination()));
            m_network.sendMessage(migrationFinish);
        } catch (NetworkException e) {
            log.error("Couldn't send migration finish message", e);
        }
    }

    private void handle(final MigrationFinish p_migrationFinish) {
        if (!p_migrationFinish.isFinished()) {
            log.warn("Migration was not successful on node {}", NodeID.toHexString(p_migrationFinish.getSource()));
        }

        MigrationIdentifier identifier = p_migrationFinish.getIdentifier();

        log.debug("ProgressMap[{}] = {}", identifier, m_progressTracker.isRunning(identifier));

        Collection<LongRange> ranges = p_migrationFinish.getLongRanges();

        log.debug("Migration {} successfully migrated chunk ranges {}", p_migrationFinish.getIdentifier(), LongRange.collectionToString(ranges));

        log.debug("Removing migrated chunks from local memory");

        m_memoryManager.lockManage();

        for (LongRange range : ranges) {
            for (long cid = range.getFrom(); cid < range.getTo(); cid++) {
                int chunkSize = m_memoryManager.remove(cid, true);
                m_backup.deregisterChunk(cid, chunkSize);
            }
        }

        m_memoryManager.unlockManage();

//        if (m_backup.isActive()) {
//            for (long cid = startId; cid <= endId; cid++) {
//                short[] backupPeers;
//                backupPeers = m_backup.getArrayOfBackupPeersForLocalChunks(cid);
//                if (backupPeers != null) {
//                    for (int j = 0; j < backupPeers.length; j++) {
//                        if (backupPeers[j] != m_boot.getNodeId() && backupPeers[j] != NodeID.INVALID_ID) {
//                            try {
//                                m_network.sendMessage(new RemoveMessage(backupPeers[j], new ArrayListLong(cid)));
//                            } catch (final NetworkException ignored) {
//                                log.warn("Sending RemoveMessage to {} failed", NodeID.toHexStringShort(backupPeers[j]));
//                            }
//                        }
//                    }
//                }
//            }
//        }



        m_progressTracker.setFinished(identifier, ranges);
    }

    /**
     * Returns the progress associated with the specified identifier or null if the identifier is not registered.
     *
     * @param p_identifier The identifier.
     * @return The progress associated with the specified identifier.
     */
    @Nullable
    public MigrationProgress getProgress(final MigrationIdentifier p_identifier) {
        return m_progressTracker.get(p_identifier);
    }

    /**
     * Returns the number of active worker threads.
     *
     * @return The number of active worker threads.
     */
    public int getWorkerCount() {
        return m_workerCount;
    }

    public void registerMessages() {

        m_network.registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE,
                MigrationMessages.SUBTYPE_MIGRATION_PUSH, MigrationPush.class);

        m_network.registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE,
                MigrationMessages.SUBTYPE_MIGRATION_FINISH, MigrationFinish.class);

        m_network.register(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_PUSH, this);

        m_network.register(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_FINISH, this);
    }

    private static class MigrationThreadFactory implements ThreadFactory {

        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        MigrationThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "migration-" + poolNumber.getAndIncrement() + "-thread-";
        }

        public Thread newThread(@NotNull Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);

            if (t.isDaemon()) {
                t.setDaemon(false);
            }

            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }

            return t;
        }
    }
}
