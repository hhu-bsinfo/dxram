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
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.migration.data.ChunkRange;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;
import de.hhu.bsinfo.dxram.migration.messages.MigrationFinish;
import de.hhu.bsinfo.dxram.migration.messages.MigrationMessages;
import de.hhu.bsinfo.dxram.migration.messages.MigrationPush;
import de.hhu.bsinfo.dxram.migration.util.BucketFuture;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.NodeID;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

public class MigrationManager implements MessageReceiver, ChunkMigrator {

    private static final Logger log = LoggerFactory.getLogger(MigrationManager.class);

    private final ExecutorService m_executor;

    private final AbstractBootComponent m_boot;
    private final BackupComponent m_backup;
    private final ChunkMigrationComponent m_chunk;
    private final LookupComponent m_lookup;
    private final MemoryManagerComponent m_memoryManager;
    private final NetworkComponent m_network;

    private final int m_workerCount;

    private final Map<MigrationIdentifier, BucketFuture> m_progressMap = new HashMap<>();

    public MigrationManager(int p_workerCount, final DXRAMComponentAccessor p_componentAccessor) {
        m_workerCount = p_workerCount;
        m_executor = Executors.newFixedThreadPool(p_workerCount, new MigrationThreadFactory());
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkMigrationComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    /**
     * Migrates the specified chunk range to the target node using multiple worker threads.
     *
     * @param p_target The target node.
     * @param p_startId The chunk range's start id.
     * @param p_endId The chunk range's end id:
     * @return A Future, which can be used for tracking progress.
     */
    public Future<Void> migrateRange(final short p_target, final long p_startId, final long p_endId) {
        MigrationIdentifier rootIdentifier = new MigrationIdentifier(m_boot.getNodeId(), p_target, p_startId, p_endId);
        MigrationTask[] tasks = createMigrationTasks(rootIdentifier);
        BucketFuture future = new BucketFuture(tasks.length);

        m_progressMap.put(rootIdentifier, future);

        for (int i = 0; i < tasks.length; i++) {
            m_executor.execute(tasks[i]);
        }

        return future;
    }

    /**
     * Creates multiple migration tasks using the specified migration identifier.
     *
     * @param p_rootIdentifier The migration identifier.
     * @return An array containing migration tasks.
     */
    public MigrationTask[] createMigrationTasks(MigrationIdentifier p_rootIdentifier) {
        MigrationTask[] tasks = new MigrationTask[m_workerCount];

        long[] partitions = partition(p_rootIdentifier.getStartId(), p_rootIdentifier.getEndId(), m_workerCount);

        MigrationIdentifier identifier;
        for (int i = 0, j = 0; i < partitions.length - 1; i += 2, j++) {
            identifier = new MigrationIdentifier(p_rootIdentifier.getSource(), p_rootIdentifier.getTarget(),
                    p_rootIdentifier.getStartId(), p_rootIdentifier.getEndId(), j);
            tasks[j] = new MigrationTask(this, identifier, partitions[i], partitions[i + 1]);
        }

        return tasks;
    }

    // TODO(krakowski)
    //  Move this method to dxutils
    public static long[] partition(long p_start, long p_end, int p_count) {
        int elementCount = (int) (p_end - p_start + 1);

        if (p_count > elementCount) {
            throw new IllegalArgumentException("Insufficient number of elements for " + p_count + " partitions");
        }

        long[] result = new long[p_count * 2];

        int length = (elementCount / p_count) + 1;
        long split = p_start;

        for (int i = 0; i < elementCount % p_count; i++) {
            result[i * 2] = length * i + p_start;
            result[(i * 2) + 1] = result[i * 2] + length - 1;
            split = result[(i * 2) + 1] + 1;
        }

        length = elementCount / p_count;
        p_start = split;

        for (int i = elementCount % p_count; i < p_count; i++) {
            result[i * 2] = length * (i - (elementCount % p_count)) + p_start;
            result[(i * 2) + 1] = result[i * 2] + length - 1;
        }

        return result;
    }

    // TODO(krakowski)
    //  Move this method to dxutils
    public static long[][] partitionChunks(long[] chunkIds, int partitions) {
        int partitionSize = (int) Math.ceil( (double) chunkIds.length / partitions );

        long[][] result = new long[partitions][];

        for (int i = 0; i < partitions; i++) {
            int index = i * partitionSize;
            int length = Math.min(chunkIds.length - index, partitionSize);
            result[i] = new long[length];
            System.arraycopy(chunkIds, index, result[i], 0, length);
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
    public Status migrate(MigrationIdentifier p_identifier, long p_startId, long p_endId) {
        int chunkCount = (int) (p_endId - p_startId + 1);

        if (m_boot.getNodeId() == p_identifier.getTarget()) {
            log.error("The migration target has to be another node");
            return Status.INVALID_ARG;
        }

        byte[][] data = new byte[chunkCount][];

        log.debug("Collecting {} chunks from memory", chunkCount);

        int index = 0;
        for (long cid = p_startId; cid <= p_endId; cid++) {
            m_memoryManager.lockAccess();

            if (!m_memoryManager.exists(cid)) {
                log.warn("Chunk {} does not exist", ChunkID.toHexString(cid));
                m_memoryManager.unlockAccess();
                throw new IllegalArgumentException("Can't migrate non-existent chunks");
            }

            // TODO(krakowski)
            //  Get a pointer pointing directly to the chunk's data
            data[index] = m_memoryManager.get(cid);
            m_memoryManager.unlockAccess();
            index++;
        }

        log.debug("Creating chunk range [{} , {}] and migration push message for node {}",
                ChunkID.toHexString(p_startId),
                ChunkID.toHexString(p_endId),
                NodeID.toHexString(p_identifier.getTarget()));

        ChunkRange chunkRange = new ChunkRange(p_startId, p_endId, data);

        MigrationPush migrationPush = new MigrationPush(p_identifier, chunkRange);

        int size = Arrays.stream(data)
                .map(a -> a.length)
                .reduce(0, (a, b) -> a + b);

        try {
            log.debug("Sending chunk range [{} , {}] to {} containing {}",
                    ChunkID.toHexString(migrationPush.getChunkRange().getStartId()),
                    ChunkID.toHexString(migrationPush.getChunkRange().getEndId()),
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
        int size = Arrays.stream(p_migrationPush.getChunkRange().getData())
                .map(a -> a.length)
                .reduce(0, (a, b) -> a + b);

        log.debug("Received chunk range [{} , {}] from {} containing {}",
                ChunkID.toHexString(p_migrationPush.getChunkRange().getStartId()),
                ChunkID.toHexString(p_migrationPush.getChunkRange().getEndId()),
                NodeID.toHexString(p_migrationPush.getSource()),
                readableFileSize(size));

        final ChunkRange chunkRange = p_migrationPush.getChunkRange();
        final long[] chunkIds = LongStream.rangeClosed(chunkRange.getStartId(), chunkRange.getEndId()).toArray();

        log.debug("Saving received chunks");

        boolean status = m_chunk.putMigratedChunks(chunkIds, chunkRange.getData());

        log.debug("Storing migrated chunks {}", status ? "succeeded" : "failed");

        final MigrationFinish migrationFinish = new MigrationFinish(p_migrationPush.getIdentifier(),
                chunkRange.getStartId(), chunkRange.getEndId(), status);

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

        log.debug("Successfully migrated chunk range [{} , {}]", ChunkID.toHexString(p_migrationFinish.getStartId()),
                ChunkID.toHexString(p_migrationFinish.getEndId()));
        log.debug("Removing migrated chunks from local memory");

        long startId = p_migrationFinish.getStartId();
        long endId = p_migrationFinish.getEndId();

        m_memoryManager.lockManage();

        for (long cid = startId; cid <= endId; cid++) {
            int chunkSize = m_memoryManager.remove(cid, true);
            m_backup.deregisterChunk(cid, chunkSize);
        }

        m_memoryManager.unlockManage();

        // TODO(krakowski)
        //  Handle backup (async)
//        if (m_backup.isActive()) {
//
//            short[] backupPeers;
//
//            backupPeers = m_backup.getArrayOfBackupPeersForLocalChunks(p_chunkID);
//
//            if (backupPeers != null) {
//
//                for (int j = 0; j < backupPeers.length; j++) {
//
//                    if (backupPeers[j] != m_boot.getNodeID() && backupPeers[j] != NodeID.INVALID_ID) {
//
//                        try {
//
//                            m_network.sendMessage(new RemoveMessage(backupPeers[j], new ArrayListLong(p_chunkID)));
//
//                        } catch (final NetworkException ignored) {
//
//                        }
//                    }
//                }
//            }
//        }

        MigrationIdentifier identifier = p_migrationFinish.getIdentifier();
        BucketFuture bucketFuture = m_progressMap.get(identifier);

        bucketFuture.setBucket(identifier.getSubId());
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
