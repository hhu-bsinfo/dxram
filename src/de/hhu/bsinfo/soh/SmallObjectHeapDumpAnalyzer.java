/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.soh;

/**
 * Analyze a heap dump of the SmallObjectHeap
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.01.2017
 */
public class SmallObjectHeapDumpAnalyzer {

    public static void main(final String[] p_args) {
        if (p_args.length < 2) {
            System.out.println("Arguments: <heap dump file> <detailed analysis: 0/1>");
            System.exit(-1);
        }

        System.out.println("Loading heap dump " + p_args[0] + "...");
        SmallObjectHeap memory = new SmallObjectHeap(p_args[0], new StorageUnsafeMemory());

        SmallObjectHeapAnalyzer analyzer = new SmallObjectHeapAnalyzer(memory);

        if (Integer.parseInt(p_args[1]) > 0) {
            System.out.println(analyzer.analyze());
        } else {
            analyzer.analyzeErrorsOnly();
        }
    }
}
