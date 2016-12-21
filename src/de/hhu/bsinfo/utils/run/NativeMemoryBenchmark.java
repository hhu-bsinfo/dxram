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

package de.hhu.bsinfo.utils.run;

import sun.misc.Unsafe;

import de.hhu.bsinfo.utils.JNINativeMemory;
import de.hhu.bsinfo.utils.UnsafeHandler;
import de.hhu.bsinfo.utils.eval.EvaluationTable;
import de.hhu.bsinfo.utils.eval.Stopwatch;

/**
 * Benchmark the JNINativeMemory implementation compiled with various
 * compilers and optimization levels and compare it against Java's Unsafe class.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public final class NativeMemoryBenchmark {

    private EvaluationTable m_table;

    /**
     * Constructor
     */
    private NativeMemoryBenchmark() {

    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Main arguments.
     */
    public static void main(final String[] p_args) {
        NativeMemoryBenchmark main = new NativeMemoryBenchmark();
        main.run(p_args);
    }

    private int run(final String[] p_args) {
        if (p_args.length < 2) {
            System.out.println("Usage: " + getClass().getSimpleName() + " [numRuns] [pathJniNativeMemory]");
        }

        final int numRuns = Integer.parseInt(p_args[0]);
        final String pathJniNativeMemory = p_args[1];

        mainEval(numRuns, pathJniNativeMemory);

        return 0;
    }

    /**
     * Execute evaluation.
     *
     * @param p_numRuns
     *     Number of runs to execute for each implementation.
     * @param p_pathJniNativeMemory
     *     Path to folder with all compiled JNINativeLibraries
     */
    private void mainEval(final int p_numRuns, final String p_pathJniNativeMemory) {
        if (p_pathJniNativeMemory.endsWith(".so") || p_pathJniNativeMemory.endsWith(".dylib")) {
            String libName = p_pathJniNativeMemory.substring(p_pathJniNativeMemory.lastIndexOf('/'), p_pathJniNativeMemory.length());
            System.out.println("Running with single library: " + libName);
            prepareTableEval(libName);

            runUnsafe(p_numRuns, "Unsafe");
            System.load(p_pathJniNativeMemory);
            runJNINativeMemory(p_numRuns, libName);
        } else {
            prepareTableEval(null);

            String path = p_pathJniNativeMemory;
            if (path.lastIndexOf('/') != path.length() - 1) {
                path += "/";
            }

            System.out.println("Running with multiple libraries from path: " + path);

            runUnsafe(p_numRuns, "Unsafe");
            System.load(path + "libJNINativeMemory_gcc_o1.so");
            runJNINativeMemory(p_numRuns, "JNINativeMemory-gcc-O1");
            System.load(path + "libJNINativeMemory_gcc_o2.so");
            runJNINativeMemory(p_numRuns, "JNINativeMemory-gcc-O2");
            System.load(path + "libJNINativeMemory_gcc_o3.so");
            runJNINativeMemory(p_numRuns, "JNINativeMemory-gcc-O3");

            System.load(path + "libJNINativeMemory_clang_o1.so");
            runJNINativeMemory(p_numRuns, "JNINativeMemory-clang-O1");
            System.load(path + "libJNINativeMemory_clang_o2.so");
            runJNINativeMemory(p_numRuns, "JNINativeMemory-clang-O2");
            System.load(path + "libJNINativeMemory_clang_o3.so");
            runJNINativeMemory(p_numRuns, "JNINativeMemory-clang-O3");

            System.load(path + "libJNINativeMemory_icc_o1.so");
            runJNINativeMemory(p_numRuns, "JNINativeMemory-intel-O1");
            System.load(path + "libJNINativeMemory_icc_o2.so");
            runJNINativeMemory(p_numRuns, "JNINativeMemory-intel-O2");
            System.load(path + "libJNINativeMemory_icc_o3.so");
            runJNINativeMemory(p_numRuns, "JNINativeMemory-intel-O3");
        }

        System.out.println(m_table.toCsv(true, "\t"));
    }

    /**
     * Create the table for measured data
     *
     * @param p_singleLibrary
     *     Name of library for the table entry
     */
    private void prepareTableEval(final String p_singleLibrary) {
        if (p_singleLibrary == null) {
            m_table = new EvaluationTable(10, 6);

            m_table.setIntersectTopCornerName("Memory");

            m_table.setRowName(0, "Unsafe");
            m_table.setRowName(1, "JNINativeMemory-gcc-O1");
            m_table.setRowName(2, "JNINativeMemory-gcc-O2");
            m_table.setRowName(3, "JNINativeMemory-gcc-O3");
            m_table.setRowName(4, "JNINativeMemory-clang-O1");
            m_table.setRowName(5, "JNINativeMemory-clang-O2");
            m_table.setRowName(6, "JNINativeMemory-clang-O3");
            m_table.setRowName(7, "JNINativeMemory-intel-O1");
            m_table.setRowName(8, "JNINativeMemory-intel-O2");
            m_table.setRowName(9, "JNINativeMemory-intel-O3");
        } else {
            m_table = new EvaluationTable(2, 6);

            m_table.setIntersectTopCornerName("Memory");

            m_table.setRowName(0, "Unsafe");
            m_table.setRowName(1, p_singleLibrary);
        }

        m_table.setColumnName(0, "create");
        m_table.setColumnName(1, "putInt");
        m_table.setColumnName(2, "getInt");
        m_table.setColumnName(3, "putByteArray");
        m_table.setColumnName(4, "getByteArray");
        m_table.setColumnName(5, "remove");
    }

    /**
     * Execute the benchmark with Java's Unsafe
     *
     * @param p_numRuns
     *     Number of runs to execute.
     * @param p_rowName
     *     Name of the row in the table to put the data into.
     */
    private void runUnsafe(final int p_numRuns, final String p_rowName) {
        Unsafe unsafe = UnsafeHandler.getInstance().getUnsafe();
        Stopwatch[] stopwatches = new Stopwatch[6];
        for (int i = 0; i < stopwatches.length; i++) {
            stopwatches[i] = new Stopwatch();
        }

        long address;
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        for (int i = 0; i < p_numRuns; i++) {
            stopwatches[0].start();
            address = unsafe.allocateMemory(data.length);
            stopwatches[0].stopAndAccumulate();

            stopwatches[1].start();
            unsafe.putInt(address, 0xAABBCCDD);
            stopwatches[1].stopAndAccumulate();

            stopwatches[2].start();
            unsafe.getInt(address);
            stopwatches[2].stopAndAccumulate();

            stopwatches[3].start();
            for (int j = 0; j < data.length; j++) {
                unsafe.putByte(address + j, data[j]);
            }
            stopwatches[3].stopAndAccumulate();

            stopwatches[4].start();
            for (int j = 0; j < data.length; j++) {
                data[j] = unsafe.getByte(address + j);
            }
            stopwatches[4].stopAndAccumulate();

            stopwatches[5].start();
            unsafe.freeMemory(address);
            stopwatches[5].stopAndAccumulate();

            // sleep to kill performance gains by continuous looping
            try {
                Thread.sleep(1);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        m_table.set(p_rowName, "create", stopwatches[0].getTimeStr());
        m_table.set(p_rowName, "putInt", stopwatches[1].getTimeStr());
        m_table.set(p_rowName, "getInt", stopwatches[2].getTimeStr());
        m_table.set(p_rowName, "putByteArray", stopwatches[3].getTimeStr());
        m_table.set(p_rowName, "getByteArray", stopwatches[4].getTimeStr());
        m_table.set(p_rowName, "remove", stopwatches[5].getTimeStr());
    }

    /**
     * Execute the benchmark with a JNINativeMemory implementation.
     *
     * @param p_numRuns
     *     Number of runs to execute.
     * @param p_rowName
     *     Name of the row in the table to put the data into.
     */
    private void runJNINativeMemory(final int p_numRuns, final String p_rowName) {
        Stopwatch[] stopwatches = new Stopwatch[6];
        for (int i = 0; i < stopwatches.length; i++) {
            stopwatches[i] = new Stopwatch();
        }

        long address;
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        for (int i = 0; i < p_numRuns; i++) {
            stopwatches[0].start();
            address = JNINativeMemory.alloc(data.length);
            stopwatches[0].stopAndAccumulate();

            stopwatches[1].start();
            JNINativeMemory.writeInt(address, 0xAABBCCDD);
            stopwatches[1].stopAndAccumulate();

            stopwatches[2].start();
            JNINativeMemory.readInt(address);
            stopwatches[2].stopAndAccumulate();

            stopwatches[3].start();
            // for (int j = 0; j < data.length; j++) {
            // JNINativeMemory.writeByte(address + j, data[j]);
            // }
            JNINativeMemory.write(address, data, 0, data.length);
            stopwatches[3].stopAndAccumulate();

            stopwatches[4].start();
            // for (int j = 0; j < data.length; j++) {
            // data[j] = JNINativeMemory.readByte(address + j);
            // }
            JNINativeMemory.read(address, data, 0, data.length);
            stopwatches[4].stopAndAccumulate();

            stopwatches[5].start();
            JNINativeMemory.free(address);
            stopwatches[5].stopAndAccumulate();

            // sleep to kill performance gains by continuous looping
            try {
                Thread.sleep(1);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        m_table.set(p_rowName, "create", stopwatches[0].getTimeStr());
        m_table.set(p_rowName, "putInt", stopwatches[1].getTimeStr());
        m_table.set(p_rowName, "getInt", stopwatches[2].getTimeStr());
        m_table.set(p_rowName, "putByteArray", stopwatches[3].getTimeStr());
        m_table.set(p_rowName, "getByteArray", stopwatches[4].getTimeStr());
        m_table.set(p_rowName, "remove", stopwatches[5].getTimeStr());
    }
}
