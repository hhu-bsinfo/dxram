package de.hhu.bsinfo.dxram.migration.progress;

import de.hhu.bsinfo.dxram.migration.LongRange;
import de.hhu.bsinfo.dxram.migration.MigrationStatus;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class MigrationProgress implements Supplier<MigrationStatus> {

    private final CountDownLatch m_countDownLatch;

    private final Set<LongRange> m_pendingRanges = ConcurrentHashMap.newKeySet();

    public MigrationProgress(final Collection<LongRange> p_pendingRanges) {
        m_pendingRanges.addAll(p_pendingRanges);
        m_countDownLatch = new CountDownLatch(m_pendingRanges.size());
    }

    @Override
    public MigrationStatus get() {
        try {
            m_countDownLatch.await();
        } catch (InterruptedException e) {
            return MigrationStatus.ERROR;
        }

        return MigrationStatus.OK;
    }

    /**
     * Sets the corresponding range's status to finished.
     *
     * @param p_range The range to finish.
     */
    public void setFinished(final LongRange p_range) {
        if (m_pendingRanges.remove(p_range)) {
            m_countDownLatch.countDown();
        }
    }

    /**
     * Returns a set containing all pending chunk ranges.
     */
    public Set<LongRange> getPendingRanges() {
        return m_pendingRanges;
    }

    /**
     * Sets the corresponding ranges' status to finished.
     *
     * @param p_ranges The ranges to finish.
     */
    public void setFinished(final Collection<LongRange> p_ranges) {
        p_ranges.forEach(this::setFinished);
    }

    public boolean isFinished() {
        return m_pendingRanges.isEmpty();
    }
}
