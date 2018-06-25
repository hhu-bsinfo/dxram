package de.hhu.bsinfo.dxram.migration.util;

import java.util.concurrent.*;

public class CountDownFuture implements Future<Void> {

    private final CountDownLatch m_countDownLatch;

    public CountDownFuture(int p_count) {

        m_countDownLatch = new CountDownLatch(p_count);
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
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {

        if (m_countDownLatch.await(timeout, unit)) {

            return null;

        } else {

            throw new TimeoutException();
        }
    }

    public void countDown() {

        m_countDownLatch.countDown();
    }
}
