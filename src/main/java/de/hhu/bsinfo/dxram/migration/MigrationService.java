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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkMigrationComponent;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.log.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;
import de.hhu.bsinfo.dxram.migration.data.MigrationPayload;
import de.hhu.bsinfo.dxram.migration.messages.MigrationFinish;
import de.hhu.bsinfo.dxram.migration.messages.MigrationMessages;
import de.hhu.bsinfo.dxram.migration.messages.MigrationPush;
import de.hhu.bsinfo.dxram.migration.progress.MigrationProgress;
import de.hhu.bsinfo.dxram.migration.progress.MigrationProgressTracker;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.ArrayListLong;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * A service providing functions for migrating chunks to other nodes.
 *
 * @author Filip Krakowski, Filip.Krakowski@Uni-Duesseldorf.de, 19.06.2018
 */
@SuppressWarnings("WeakerAccess")
@Module.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class MigrationService extends Service<MigrationServiceConfig> implements MessageReceiver, ChunkMigrator {

    public static final ThreadFactory THREAD_FACTORY = new MigrationThreadFactory();

    private ExecutorService m_executor;

    private final BootComponent m_boot;
    private final BackupComponent m_backup;
    private final ChunkMigrationComponent m_chunkMigration;
    private final ChunkComponent m_chunk;
    private final NetworkComponent m_network;
    private final LookupComponent m_lookup;

    private int m_workerCount = 16;

    private final MigrationProgressTracker m_progressTracker = new MigrationProgressTracker();

    private static final int SHUTDOWN_TIMEOUT = 5000;
    private static final TimeUnit SHUTDOWN_TIMEUNIT = TimeUnit.MILLISECONDS;

    public MigrationService(final ComponentProvider p_componentProvider) {
        m_boot = p_componentProvider.getComponent(BootComponent.class);
        m_backup = p_componentProvider.getComponent(BackupComponent.class);
        m_chunk = p_componentProvider.getComponent(ChunkComponent.class);
        m_chunkMigration = p_componentProvider.getComponent(ChunkMigrationComponent.class);
        m_network = p_componentProvider.getComponent(NetworkComponent.class);
        m_lookup = p_componentProvider.getComponent(LookupComponent.class);
    }

    public MigrationTicket migrateRange(final long p_from, final long p_to, final short p_target) {
        return migrateRange(p_target, new LongRange(p_from, p_to));
    }

    /**
     * Migrates the specified chunk range to the target node using multiple worker threads.
     *
     * @param p_target
     *         The target node.
     * @param p_range
     *         The chunk range.
     * @return A ticket containing information associated with the created migration.
     */
    public MigrationTicket migrateRange(final short p_target, final LongRange p_range) {

        LOGGER.info("Migrating chunk range [%X,%X] to target node %04X", p_range.getFrom(), p_range.getTo(), p_target);

        MigrationIdentifier identifier = new MigrationIdentifier(m_boot.getNodeId(), p_target);
        List<MigrationTask> tasks = createMigrationTasks(identifier, p_range);

        CompletableFuture<MigrationStatus> future = m_progressTracker.register(identifier, tasks.stream()
                .flatMap(task -> task.getRanges().stream()).collect(Collectors.toList()));

        tasks.forEach(m_executor::execute);

        return new MigrationTicket(future, identifier);
    }

    /**
     * Creates multiple migration tasks using the specified migration identifier.
     *
     * @param p_identifier
     *         The migration identifier.
     * @return An array containing migration tasks.
     */
    public List<MigrationTask> createMigrationTasks(MigrationIdentifier p_identifier, LongRange p_range) {
        List<MigrationTask> tasks = new ArrayList<>(m_workerCount);

        long[] partitions = partition(p_range.getFrom(), p_range.getTo(), m_workerCount);

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
        m_executor.execute(() -> {
            switch (p_message.getSubtype()) {
                case MigrationMessages.SUBTYPE_MIGRATION_PUSH:
                    handle((MigrationPush) p_message);
                    break;
                case MigrationMessages.SUBTYPE_MIGRATION_FINISH:
                    handle((MigrationFinish) p_message);
                    break;
                default:
                    break;
            }
        });
    }

    @Override
    public void onStatus(MigrationIdentifier p_identifier, long p_startId, long p_endId, Status p_result) {
        LOGGER.debug("Received Result %s for chunk range [%X, %X]", p_result, p_startId, p_endId);
    }

    @Override
    public Status migrate(MigrationIdentifier p_identifier, List<LongRange> p_ranges) {
        int chunkCount = LongRange.collectionToSize(p_ranges);

        if (m_boot.getNodeId() == p_identifier.getTarget()) {
            LOGGER.error("The migration target has to be another node");
            return Status.INVALID_ARG;
        }

        byte[][] data = new byte[chunkCount][];

        LOGGER.debug("Collecting %d chunks from memory", chunkCount);

        int index = 0;
        for (LongRange range : p_ranges) {
            for (long chunkId = range.getFrom(); chunkId < range.getTo(); chunkId++) {

                if ((data[index] = m_chunk.getMemory().get().get(chunkId).getData()) == null) {
                    LOGGER.warn("Chunk %X does not exist", chunkId);
                    throw new IllegalArgumentException("Can't migrate non-existent chunks");
                }

                index++;
            }
        }

        LOGGER.debug("Creating chunk ranges %s and migration push message for node %X",
                LongRange.collectionToString(p_ranges),
                p_identifier.getTarget());

        MigrationPayload migrationPayload = new MigrationPayload(p_ranges, data);

        MigrationPush migrationPush = new MigrationPush(p_identifier, migrationPayload);

        int size = Arrays.stream(data)
                .map(a -> a.length)
                .reduce(0, (a, b) -> a + b);

        try {
            LOGGER.debug("Sending chunk ranges %s to %X containing %s",
                    LongRange.collectionToString(p_ranges),
                    migrationPush.getDestination(),
                    readableFileSize(size));

            m_network.sendMessage(migrationPush);
        } catch (NetworkException e) {
            LOGGER.error("Couldn't send migration push to target", e);
            return Status.NOT_SENT;
        }

        return Status.SENT;
    }

    public static String readableFileSize(long p_size) {
        if (p_size <= 0) {
            return "0";
        }

        final String[] units = new String[] {"B", "kB", "MB", "GB", "TB"};
        int digitGroups = (int) (Math.log10(p_size) / Math.log10(1024));
        return new DecimalFormat("###0.#").format(p_size / Math.pow(1024, digitGroups)) + ' ' + units[digitGroups];
    }

    private void handle(final MigrationPush p_migrationPush) {
        final MigrationPayload payload = p_migrationPush.getPayload();
        int size = payload.getSize();

        List<LongRange> ranges = payload.getLongRanges();

        LOGGER.debug("Received chunk range %s from %X containing %s",
                LongRange.collectionToString(ranges),
                p_migrationPush.getDestination(),
                readableFileSize(size));

        final long[] chunkIds = ranges.stream().flatMapToLong(range -> LongStream.range(range.getFrom(), range.getTo()))
                .toArray();

        LOGGER.debug("Saving received chunks");

        boolean status = m_chunkMigration.putMigratedChunks(chunkIds, payload.getData());

        LOGGER.debug("Storing migrated chunks %s", status ? "succeeded" : "failed");

        final MigrationFinish migrationFinish = new MigrationFinish(p_migrationPush.getIdentifier(), ranges, status);

        try {
            LOGGER.debug("Sending response to %X", migrationFinish.getDestination());
            m_network.sendMessage(migrationFinish);
        } catch (NetworkException e) {
            LOGGER.error("Couldn't send migration finish message", e);
        }
    }

    private void handle(final MigrationFinish p_migrationFinish) {
        if (!p_migrationFinish.isFinished()) {
            LOGGER.warn("Migration was not successful on node %X", p_migrationFinish.getSource());
        }

        MigrationIdentifier identifier = p_migrationFinish.getIdentifier();

        LOGGER.debug("ProgressMap[%s] = %b", identifier.toString(), m_progressTracker.isRunning(identifier));

        Collection<LongRange> ranges = p_migrationFinish.getLongRanges();

        LOGGER.debug("Migration %s successfully migrated chunk ranges %s", p_migrationFinish.getIdentifier().toString(),
                LongRange.collectionToString(ranges));

        LOGGER.debug("Removing migrated chunks from local memory");

        // Remove chunks from local storage and inform superpeer about migration
        short target = p_migrationFinish.getSource();
        for (LongRange range : ranges) {
            for (long cid = range.getFrom(); cid < range.getTo(); cid++) {
                int chunkSize = m_chunk.getMemory().remove().remove(cid, true);
                m_backup.deregisterChunk(cid, chunkSize);
            }
            m_lookup.migrateRange(range.getFrom(), range.getTo(), target);
        }


        // Remove chunks on remote backup peers
        if (m_backup.isActive()) {
            for (LongRange range : ranges) {
                for (long cid = range.getFrom(); cid < range.getTo(); cid++) {
                    short[] backupPeers;
                    backupPeers = m_backup.getArrayOfBackupPeersForLocalChunks(cid);
                    if (backupPeers != null) {
                        for (int j = 0; j < backupPeers.length; j++) {
                            if (backupPeers[j] != m_boot.getNodeId() && backupPeers[j] != NodeID.INVALID_ID) {
                                try {
                                    m_network.sendMessage(new RemoveMessage(backupPeers[j], new ArrayListLong(cid)));
                                } catch (final NetworkException ignored) {
                                    LOGGER.warn("Sending RemoveMessage to %X failed", backupPeers[j]);
                                }
                            }
                        }
                    }
                }
            }
        }

        m_progressTracker.setFinished(identifier, ranges);
    }

    /**
     * Returns the progress associated with the specified identifier or null if the identifier is not registered.
     *
     * @param p_identifier
     *         The identifier.
     * @return The progress associated with the specified identifier.
     */
    public @Nullable MigrationProgress getProgress(final MigrationIdentifier p_identifier) {
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

    /**
     * Waits until the corresponding migration finishes.
     *
     * @param p_ticket
     *         The ticket associated with the migration.
     * @return The migration's status or null if an exception occurred.
     */
    public @Nullable MigrationStatus await(final @NotNull MigrationTicket p_ticket) {
        try {
            return p_ticket.getFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warn("Waiting on migration failed", e);
            return null;
        }
    }

    /**
     * Waits until the corresponding migration finishes or the specified timeout is reached.
     *
     * @param p_ticket
     *         The ticket associated with the migration.
     * @return The migration's status or null if an exception occurred or the timeout was reached.
     */
    public @Nullable MigrationStatus await(final long p_timeout, final @NotNull TimeUnit p_timeUnit,
            final @NotNull MigrationTicket p_ticket) {
        try {
            return p_ticket.getFuture().get(p_timeout, p_timeUnit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.warn("Waiting on migration failed", e);
            return null;
        }
    }

    /**
     * Registers all message types within the network component.
     */
    void registerMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE,
                MigrationMessages.SUBTYPE_MIGRATION_PUSH, MigrationPush.class);
        m_network.registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE,
                MigrationMessages.SUBTYPE_MIGRATION_FINISH, MigrationFinish.class);

        m_network.register(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_PUSH, this);
        m_network.register(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_FINISH, this);
    }

    @Override
    protected void resolveComponentDependencies(ComponentProvider p_componentAccessor) {

    }

    @Override
    protected boolean startService(DXRAMConfig p_config) {
        m_executor = Executors.newFixedThreadPool(getConfig().getWorkerCount(), THREAD_FACTORY);
        registerMessages();
        return true;
    }

    @Override
    protected boolean shutdownService() {
        m_executor.shutdown();

        boolean terminated = false;

        try {
            terminated = m_executor.awaitTermination(SHUTDOWN_TIMEOUT, SHUTDOWN_TIMEUNIT);
        } catch (InterruptedException e) {
            // ignored
        }

        if (!terminated) {
            m_executor.shutdownNow();
        }

        return true;
    }

    // TODO(krakowski)
    //  Move this to dxutils as NamedThreadFactory
    private static class MigrationThreadFactory implements ThreadFactory {

        private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
        private final ThreadGroup m_group;
        private final AtomicInteger m_threadNumber = new AtomicInteger(1);
        private final String m_prefix;

        MigrationThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            m_group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            m_prefix = "migration-" + POOL_NUMBER.getAndIncrement() + "-thread-";
        }

        public Thread newThread(@NotNull Runnable r) {
            Thread t = new Thread(m_group, r, m_prefix + m_threadNumber.getAndIncrement(), 0);

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
