/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxgraph.conv;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Binary edge buffer implementation based on a lock free ring buffer.
 * One writer/adder and many consumers/readers supported.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.05.2016
 */
class BinaryEdgesRingBuffer implements BinaryEdgeBuffer {
    private long[] m_buffer;
    private int m_size;

    private AtomicInteger m_posFront;
    private AtomicInteger m_posBack;

    // size = num edges in buffer
    BinaryEdgesRingBuffer(final int p_size) {
        m_size = p_size;
        m_buffer = new long[p_size * 2];

        m_posFront = new AtomicInteger(0);
        m_posBack = new AtomicInteger(0);
    }

    // public static void main(String[] args) {
    // BinaryEdgesRingBuffer buffer = new BinaryEdgesRingBuffer(100);
    //
    // Thread filler = new Thread() {
    // @Override
    // public void run() {
    // long counter = 0;
    // while (true) {
    // if (!buffer.pushBack(counter++, counter++)) {
    // //System.out.println("Full, waiting...");
    // Thread.yield();
    // }
    // }
    // }
    // };
    // filler.start();
    //
    // Thread[] consumer = new Thread[8];
    // for (int i = 0; i < consumer.length; i++) {
    // consumer[i] = new Thread() {
    // @Override
    // public void run() {
    // long[] vals = new long[2];
    // while (true) {
    // int res = buffer.popFront(vals);
    // if (res == -1) {
    // //System.out.println(Thread.currentThread().getId() + " failed pop, retry");
    // } else if (res == 0) {
    // //System.out.println(Thread.currentThread().getId() + " empty, wait...");
    // Thread.yield();
    // } else {
    //
    // if (vals[0] + 1 != vals[1]) {
    // throw new RuntimeException("Sync error: " + vals[0] + "/" + vals[1]);
    // } else {
    // //System.out.println("Success " + vals[0] + "/" + vals[1]);
    // }
    // }
    // }
    // }
    // };
    // consumer[i].start();
    // }
    // }

    // one thread, only
    @Override
    public boolean pushBack(final long p_val, final long p_val2) {
        int posBack = m_posBack.get();

        if (((posBack + 1) % m_size) == (m_posFront.get() % m_size)) {
            return false;
        }

        int posBack2 = posBack % m_size;
        m_buffer[(posBack2 * 2) % m_buffer.length] = p_val;
        m_buffer[(posBack2 * 2 + 1) % m_buffer.length] = p_val2;
        m_posBack.set(posBack + 1);
        return true;
    }

    // multiple threads
    @Override
    public int popFront(final long[] p_retVals) {
        int posFront = m_posFront.get();
        int posFront2 = posFront % m_size;

        // empty
        if (posFront2 == (m_posBack.get() % m_size)) {
            return 0;
        }

        long val1 = m_buffer[(posFront2 * 2) % m_buffer.length];
        long val2 = m_buffer[(posFront2 * 2 + 1) % m_buffer.length];

        if (!m_posFront.compareAndSet(posFront, posFront + 1)) {
            return -1;
        }

        p_retVals[0] = val1;
        p_retVals[1] = val2;

        return 2;
    }

    @Override
    public boolean isEmpty() {
        return m_posFront.get() == m_posBack.get();
    }
}
