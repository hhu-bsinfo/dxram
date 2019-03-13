package de.hhu.bsinfo.dxram.migration.progress;

import de.hhu.bsinfo.dxram.migration.LongRange;
import de.hhu.bsinfo.dxram.migration.MigrationService;
import de.hhu.bsinfo.dxram.migration.MigrationStatus;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MigrationProgressTracker {

    private final ExecutorService m_completableExecutor = Executors.newSingleThreadExecutor(MigrationService.THREAD_FACTORY);

    private final Map<MigrationIdentifier, MigrationProgress> m_progressMap = new ConcurrentHashMap<>();

    public MigrationProgressTracker() {

    }

    public CompletableFuture<MigrationStatus> register(final MigrationIdentifier p_identifier, final Collection<LongRange> p_ranges) {
        MigrationProgress progress = new MigrationProgress(p_ranges);
        m_progressMap.put(p_identifier, progress);
        return CompletableFuture.supplyAsync(progress, m_completableExecutor);
    }

    /**
     * Removes the specified chunk ranges from progress tracking under the specified identifier.
     *
     * @param p_identifier The identifier.
     * @param p_ranges The chunk ranges.
     */
    public void setFinished(final MigrationIdentifier p_identifier, final Collection<LongRange> p_ranges) {
        MigrationProgress progress = m_progressMap.get(p_identifier);

        if (progress == null) {
            return;
        }

        progress.setFinished(p_ranges);

        if (progress.isFinished()) {
            m_progressMap.remove(p_identifier);
        }
    }

    public void setError(final @NotNull MigrationIdentifier p_identifier) {
        MigrationProgress progress = m_progressMap.get(p_identifier);

        if (progress == null) {
            return;
        }

        progress.onError();
        m_progressMap.remove(p_identifier);
    }

    /**
     * Indicates if the specified migration is still being performed.
     *
     * @param p_identifier The identifier.
     * @return True, if the migration is still running; false else
     */
    public boolean isRunning(final MigrationIdentifier p_identifier) {
        return m_progressMap.containsKey(p_identifier);
    }

    /**
     * Returns the progress associated with the specified identifier.
     *
     * @param p_identifier The identifier.
     * @return The progress associated with the specified identifier.
     */
    public MigrationProgress get(final MigrationIdentifier p_identifier) {
        return m_progressMap.get(p_identifier);
    }
}
