package de.hhu.bsinfo.dxram.migration.progress;

import de.hhu.bsinfo.dxram.migration.LongRange;
import de.hhu.bsinfo.dxram.migration.MigrationManager;
import de.hhu.bsinfo.dxram.migration.MigrationStatus;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MigrationProgressTracker {

    private final ExecutorService m_completableExecutor = Executors.newSingleThreadExecutor(MigrationManager.THREAD_FACTORY);

    private final Map<MigrationIdentifier, MigrationProgress> m_progressMap = new ConcurrentHashMap<>();

    public MigrationProgressTracker() {

    }

    public CompletableFuture<MigrationStatus> register(final MigrationIdentifier p_identifier, final Collection<LongRange> p_ranges) {
        MigrationProgress progress = new MigrationProgress(p_ranges);
        m_progressMap.put(p_identifier, progress);
        return CompletableFuture.supplyAsync(progress, m_completableExecutor);
    }

    public void setFinished(final MigrationIdentifier p_identifier, final Collection<LongRange> p_ranges) {
        MigrationProgress progress = m_progressMap.get(p_identifier);
        progress.setFinished(p_ranges);

        if (progress.isFinished()) {
            m_progressMap.remove(p_identifier);
        }
    }

    public boolean isRegistered(final MigrationIdentifier p_identifier) {
        return m_progressMap.containsKey(p_identifier);
    }
}
