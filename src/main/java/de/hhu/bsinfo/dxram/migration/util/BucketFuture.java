package de.hhu.bsinfo.dxram.migration.util;

import org.jetbrains.annotations.NotNull;

import java.util.BitSet;
import java.util.concurrent.*;

public class BucketFuture implements Future<Void> {

    private final CountDownLatch m_countDownLatch;

    private final BitSet m_bitSet;

    private final int m_count;

    public BucketFuture(int p_count) {
        m_countDownLatch = new CountDownLatch(1);
        m_bitSet = new BitSet(p_count);
        m_count = p_count;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return m_countDownLatch.getCount() == 0;
    }

    @Override
    public Void get() throws InterruptedException {
        m_countDownLatch.await();
        return null;
    }

    @Override
    public Void get(long p_timeout, @NotNull TimeUnit p_unit) throws InterruptedException, TimeoutException {
        if (m_countDownLatch.await(p_timeout, p_unit)) {
            return null;
        } else {
            throw new TimeoutException();
        }
    }

    /**
     * Sets the corresponding bucket.
     *
     * @param p_index The bucket index to set.
     */
    public void setBucket(final int p_index) {
        if (p_index >= m_count) {
            throw new IndexOutOfBoundsException();
        }

        // This does not need to be locked, since only one bit is set.
        m_bitSet.set(p_index);

        if (m_bitSet.cardinality() == m_count) {
            m_countDownLatch.countDown();
        }
    }
}
