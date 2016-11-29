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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

/* Copyright (c) 2013 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/*
 * This is a re-implementation of the LSF simulator described in Mendel Rosenblum's
 * PhD dissertation. Assuming it's correct, it appears to show that Mendel's
 * simulator actually implemented a better cost-benefit policy than what was
 * described in the text and implemented in Sprite LSF. See Steve Rumble's
 * dissertation for details.
 *
 * The LSF simulator was ported to Java and extended.
 */

/**
 * LSFSimulator
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.03.2014
 */
public final class LSFSimulator {

    // Constants
    // These are LSF simulator defaults (see 5.5.3 in Mendel's dissertation)
    // Sentinel. Do not change.
    private static final int DEAD_OBJECT_ID = -1;
    // Simulates 4KB objects and 2MB segments.
    private static final int OBJECTS_PER_SEGMENT = 512;
    // 100 segments of live data
    private static final int TOTAL_LIVE_OBJECTS = 51200;
    // Controls amount of live data processed per cleaning pass.
    private static final int MAX_LIVE_OBJECTS_WRITTEN_PER_PASS = 2560;

    // Attributes
    // Usually a multiple of OBJECTS_PER_SEGMENT.
    // Cost-benefit uses the min (not average) object age.
    private static boolean ms_orderSegmentsByMinAge;
    // Cost-benefit uses the segment (rather than object) age.
    private static boolean ms_orderSegmentsBySegmentAge;
    // For reasons not well-understood, this helps cost-benefit sometimes.
    private static boolean ms_takeRootOfAge;
    // For reasons not well-understood, this helps cost-benefit sometimes.
    private static boolean ms_takeLogOfAge;
    // Set to true to sort survivors. Most of Mendel's experiments
    private static boolean ms_sortSurvivorObjects;
    // appear to have set this to true, but I don't think they all
    // did (e.g. the initial simulations in 5-2).
    // If true, set object timestamps become 'currentTimestamp' when moved.
    private static boolean ms_resetObjectTimestampsWhenCleaning;
    // If true, use RAMCloud's slightly different cost-benefit equation.
    private static boolean ms_useRamcloudCostBenefit;
    // If true, use DXRAM's slightly different cost-benefit equation.
    private static boolean ms_useDxramCostBenefit;
    // If true, use segment decay rate rather than data age in cost-benefit.
    private static boolean ms_useDecayForCostBenefit;
    // If true and we're replaying a script, use actual lifetimes rather than estimates based on age.
    private static boolean ms_useLifetimesWhenReplaying;
    // If TAKE_ROOT_OF_AGE is true, this is what age will be raised to.
    private static double ms_rootExp;
    // If TAKE_LOG_OF_AGE is true, this is the base of our log.
    private static double ms_logBase;

    // -Counters-
    private static int ms_segmentsCleaned;
    private static int ms_cleaningPasses;
    private static int ms_segmentsFreeAfterCleaning;
    private static int ms_emptySegmentsCleaned;
    private static int ms_newObjectsWritten;
    private static int ms_cleanerObjectsWritten;
    private static int ms_currentTimestamp;
    private static int[] ms_objectWriteCounts;

    // -Commandline parameters-
    private static Distribution ms_distribution;
    private static Strategy ms_strategy;

    private static int ms_utilisation;
    private static int ms_line;
    private static long ms_lastN;
    private static double ms_lastS;
    private static double ms_cachedResult;
    private static boolean ms_generated;
    private static ArrayList<Integer> ms_fillIds;
    private static Random ms_random;

    private static Utilisations ms_preCleaningUtilisations;
    private static Utilisations ms_cleanedSegmentUtilisations;

    // Constructors

    /**
     * Creates an instance of LSFSimulator
     */
    private LSFSimulator() {
    }

    // Methods

    /**
     * ...
     *
     * @param p_util
     *         ...
     * @param p_list
     *         ...
     */
    public static void recordUtilisations(final Utilisations p_util, final ArrayList<Segment> p_list) {

        for (int i = 0; i < p_list.size(); i++) {
            p_util.store(p_list.get(i).utilisation());
        }
    }

    /**
     * ...
     *
     * @param p_fileReader
     *         ...
     * @return ...
     */
    public static int[] nextObject(final FileReader p_fileReader) {
        int[] ret;
        String str = "";
        String[] splits;
        BufferedReader reader;

        ret = new int[2];
        if (p_fileReader != null) {
            // If we're replaying a script, just return the next value from it.
            reader = new BufferedReader(p_fileReader);

            try {
                str = reader.readLine();
                if (str != null) {
                    splits = str.split(" ");
                    ret[0] = Integer.parseInt(splits[0]);
                    ret[1] = Integer.parseInt(splits[1]);
                    if (ret[1] == -1) {
                        // -1 means that the file is never overwritten again
                        ret[1] = Integer.MAX_VALUE;
                    }
                    assert ret[1] >= 0;
                    ms_line++;
                } else {
                    System.out.format("corrupt script file: line #%d\n", ms_line);
                    System.exit(1);
                }
            } catch (final NumberFormatException | IOException e) {
                System.out.format("corrupt script file: line #%d\n", ms_line);
                System.exit(1);
            }
        } else {
            // Otherwise, we're generating the accesses from the given distribution.

            // Pre-filling by first writing each file before doing any overwrites is
            // what Mendel's dissertation says happens (5.3.1). Diego wondered if this
            // makes a difference. It doesn't appear to matter much if one prefills all
            // objects or uses the distribution the whole time.
            //
            // However, for distributions with substantial amounts of very cold data
            // (like exponential), we need to be careful to randomly prefill. If we
            // don't, we can end up packing together objects that will never be
            // overwritten into perfectly cold segments from the get-go, which makes
            // the behaviour appear artificially good. This occurs because Distribution
            // often chooses objects such that objects with close ids have similar
            // expected lifetimes.

            if (ms_fillIds.isEmpty() && !ms_generated) {
                for (int i = 0; i < TOTAL_LIVE_OBJECTS; i++) {
                    ms_fillIds.add(i);
                }
                Collections.shuffle(ms_fillIds, ms_random);
                ms_generated = true;
            }
            if (!ms_fillIds.isEmpty()) {
                ret[0] = ms_fillIds.remove(ms_fillIds.size() - 1);
            } else {
                ret[0] = ms_distribution.getNextObject();
            }
            ret[1] = -1;
        }
        return ret;
    }

    /**
     * ...
     *
     * @param p_activeList
     *         ...
     * @param p_segmentsToClean
     *         ...
     */
    public static void getSegmentsToClean(final ArrayList<Segment> p_activeList, final ArrayList<Segment> p_segmentsToClean) {
        ms_strategy.chooseSegments(p_activeList, p_segmentsToClean);
        recordUtilisations(ms_cleanedSegmentUtilisations, p_segmentsToClean);
    }

    /**
     * ...
     *
     * @param p_segments
     *         ...
     * @param p_liveObjects
     *         ...
     */
    public static void getLiveObjects(final ArrayList<Segment> p_segments, final ArrayList<Obj> p_liveObjects) {
        Segment s;

        for (int i = 0; i < p_segments.size(); i++) {
            s = p_segments.get(i);
            for (int j = 0; j < s.size(); j++) {
                if (s.m_objects.get(j).isDead()) {
                    continue;
                }
                p_liveObjects.add(s.m_objects.get(j));
            }
        }

        if (ms_sortSurvivorObjects) {
            if (ms_useLifetimesWhenReplaying) {
                Collections.sort(p_liveObjects, new LifetimeComparator());
            } else {
                Collections.sort(p_liveObjects, new TimeStampComparator());
            }
        }
    }

    /**
     * ...
     *
     * @param p_activeList
     *         ...
     * @param p_freeList
     *         ...
     * @param p_objectToSegment
     *         ...
     * @return ...
     */
    public static Segment clean(final ArrayList<Segment> p_activeList, final ArrayList<Segment> p_freeList,
            final ArrayList<ObjectReference> p_objectToSegment) {
        int survivorsAllocated = 0;
        int id;
        long timeOfDeath;
        long timestamp;
        Segment newHead = null;
        Segment s;
        ArrayList<Segment> survivorList;
        ArrayList<Segment> segmentsToClean;
        ArrayList<Obj> liveObjects;

        // We don't clean to the current head segment for the simple reason that
        // it is full (cleaning occurs only when we've completely run out of
        // free segments).
        survivorList = new ArrayList<Segment>();
        ms_cleaningPasses++;
        recordUtilisations(ms_preCleaningUtilisations, p_activeList);

        segmentsToClean = new ArrayList<Segment>();
        getSegmentsToClean(p_activeList, segmentsToClean);

        liveObjects = new ArrayList<Obj>();
        getLiveObjects(segmentsToClean, liveObjects);
        ms_cleanerObjectsWritten += liveObjects.size();

        for (int i = 0; i < liveObjects.size(); i++) {
            assert !liveObjects.get(i).isDead();

            if (newHead != null && newHead.full()) {
                survivorList.add(newHead);
                newHead = null;
            }

            if (newHead == null) {
                newHead = new Segment();
                survivorsAllocated++;
            }

            timestamp = liveObjects.get(i).getTimestamp();

            // It seems like you wouldn't want to do this, but it actually
            // appears to give better results with the LSF cost-benefit
            // equation. I suspect what's happening is that it prevents
            // generating rare, but extremely cold and old segments that
            // are cleaned more often than is desirable.
            //
            // Perhaps this is what Mendel's simulator used to do?
            //
            // The downside is that it retards sorting of objects by age.
            // See the ORDER_SEGMENTS_BY_SEGMENT_AGE flag for a better
            // method that does the same thing in terms of C-B, but keeps
            // absolute ages when sorting.
            if (ms_resetObjectTimestampsWhenCleaning) {
                timestamp = ms_currentTimestamp;
            }

            id = liveObjects.get(i).getId();
            timeOfDeath = liveObjects.get(i).getTimeOfDeath();
            p_objectToSegment.set(id, newHead.append(id, timestamp, timeOfDeath));
        }

        for (int i = 0; i < segmentsToClean.size(); i++) {
            s = segmentsToClean.get(i);

            for (int j = 0; j < p_objectToSegment.size(); j++) {
                assert p_objectToSegment.get(j).m_segment != s;
            }

            ms_segmentsCleaned++;
            if (s.liveObjects() == 0) {
                ms_emptySegmentsCleaned++;
            }

            // Only "return" this segment to the free list once we've
            // already made up for any segments allocated during this
            // cleaning pass.
            if (survivorsAllocated > 0) {
                survivorsAllocated--;
            } else {
                p_freeList.add(new Segment());
            }
        }

        assert survivorsAllocated == 0;

        for (int i = 0; i < survivorList.size(); i++) {
            p_activeList.add(survivorList.get(i));
        }

        ms_segmentsFreeAfterCleaning += p_freeList.size();

        return newHead;
    }

    /**
     * ...
     *
     * @return ...
     */
    public static double lsfWriteCost() {
        return (double) (ms_newObjectsWritten + ms_cleanerObjectsWritten + (ms_segmentsCleaned - ms_emptySegmentsCleaned) * OBJECTS_PER_SEGMENT) /
                (double) ms_newObjectsWritten;
    }

    /**
     * ...
     *
     * @return ...
     */
    public static double ramcloudWriteCost() {
        return (double) (ms_newObjectsWritten + ms_cleanerObjectsWritten) / (double) ms_newObjectsWritten;
    }

    /**
     * ...
     *
     * @param p_writes
     *         ...
     * @param p_fileWriter
     *         ...
     */
    public static void generateScriptFile(final ArrayList<Integer> p_writes, final FileWriter p_fileWriter) {
        // Our vector contains the full ordered history of writes from this run.
        // We want to scan it and compute the time of death of each object (when it
        // is overwritten in the future), then we'll dump an ascii file of integer
        // <objectId, timeOfDeath> pairs for a future invocation of the simulator to
        // replay.

        int lastIndex;
        int objId;
        ArrayList<Pair> idAndTimeOfDeath;
        ArrayList<Integer> lastWriteIndexMap;
        BufferedWriter writer;

        // This is the vector we'll generate and dump. It contains <id, timeOfDeath>
        // tuples.
        idAndTimeOfDeath = new ArrayList<Pair>(p_writes.size());

        // This map references the previous write of each object. It is used to
        // record the lifetime of an object when we encounter the next one that
        // overwrites of it.
        lastWriteIndexMap = new ArrayList<Integer>(TOTAL_LIVE_OBJECTS);

        for (int i = 0; i < p_writes.size(); i++) {
            objId = p_writes.get(i);
            idAndTimeOfDeath.set(i, new Pair(objId, -1));
            lastIndex = lastWriteIndexMap.get(objId);
            if (lastIndex != -1) {
                // When the object was written is implicit in the order.
                assert i > lastIndex;
                assert idAndTimeOfDeath.get(lastIndex).getFirstValue() == objId;
                idAndTimeOfDeath.get(lastIndex).setSecondValue(i);
            }
            lastWriteIndexMap.set(objId, i);
        }

        try {
            writer = new BufferedWriter(p_fileWriter);
            for (int i = 0; i < idAndTimeOfDeath.size(); i++) {
                writer.write(idAndTimeOfDeath.get(i).getFirstValue() + " " + idAndTimeOfDeath.get(i).getSecondValue() + "\n");
            }
            writer.close();
        } catch (final IOException e) {
            System.out.println("error while writing to file");
            System.exit(1);
        }
    }

    /**
     * Dump a histogram of object writes to stderr so that the access distribution
     * can be sanity checked.
     * The output is as follows:
     * [% of total objects] [probability %] [cumulative %]
     * The first column serves as an x-value. The second and third columns are
     * y-values that can be used for PDFs and CDFs, respectively.
     */
    public static void dumpObjectWriteHistogram() {
        int temp;

        System.out.println("####\n# Object Write Distribution\n####");

        Arrays.sort(ms_objectWriteCounts);
        for (int i = 0; i < ms_objectWriteCounts.length / 2; i++) {
            temp = ms_objectWriteCounts[i];
            ms_objectWriteCounts[i] = ms_objectWriteCounts[ms_objectWriteCounts.length - (i + 1)];
            ms_objectWriteCounts[ms_objectWriteCounts.length - (i + 1)] = temp;
        }

        int sum = 0;
        int cumulativeSum = 0;
        for (int i = 0; i < TOTAL_LIVE_OBJECTS; i++) {
            sum += ms_objectWriteCounts[i];
            cumulativeSum += ms_objectWriteCounts[i];
            if (i != 0 && i % (TOTAL_LIVE_OBJECTS / 100) == 0) {
                System.out.format("%.6f %.6f %.6f\n", (double) i / (TOTAL_LIVE_OBJECTS / 100) / 100, (double) sum / ms_newObjectsWritten,
                        (double) cumulativeSum / ms_newObjectsWritten);
                sum = 0;
            }
        }
    }

    /**
     * ...
     */
    public static void usage() {
        System.out.println("valid parameters include:");
        System.out.println("  -a                                           " + "(make cost-benefit use average age, not min)");
        System.out.println("  -A                                           " + "(make cost-benefit use segment, not object, age)");
        System.out.println("  -d hotAndCold|uniform|exponential|zipfian " + "(the access distribution)");
        System.out.println("  -D                                           " + "(use segment decay rate in cost-benefit strategy)");
        System.out.println("  -o scriptFile                                " + "(dump the full list of object ids written here)");
        System.out.println("  -i scriptFile                                " + "(replay the given script of object writes)");
        System.out.println("  -l                                           " + "(if replaying from script, use actual object lifetimes in cleaning)");
        System.out.println("  -L base                                      " + "(if using cost-benefit, take the log of age with given base)");
        System.out.println("  -r                                           " + "(do not reorder live objects by age when cleaning)");
        System.out.println("  -f rc|dx                                     " + "(use RAMCloud or DxRAM equation for the costBenefit strategy)");
        System.out.println("  -s greedy|costBenefit                        " + "(the segment selection strategy)");
        System.out.println("  -S exponent                                  " + "(if using cost-benefit, take the root of age with given exponent)");
        System.out.println("  -t                                           " + "(reset object timestamps when moved during cleaning)");
        System.out.println("  -u utilisation                               " + "(%% of memory utilisation; 1-99%%)");
        System.exit(1);
    }

    /**
     * Main method
     *
     * @param p_arguments
     *         the command line arguments
     */
    public static void main(final String[] p_arguments) {
        int i = 0;
        int liveDataSegments;
        int totalSegments;
        int writesSinceChange;
        int obj;
        int lifetime;
        int[] retValues;
        long lastPrintTime;
        long startTime;
        long runTime;
        double lastWc;
        double wcLSF;
        double wcRC;
        Segment head = null;
        ArrayList<Segment> freeList;
        ArrayList<Segment> activeList;
        ArrayList<ObjectReference> objectToSegment;
        ArrayList<Integer> writes;
        FileWriter outScriptFile = null;
        FileReader inScriptFile = null;

        freeList = new ArrayList<Segment>();
        activeList = new ArrayList<Segment>();
        objectToSegment = new ArrayList<ObjectReference>();
        writes = new ArrayList<Integer>();

        // Initialize global variables
        ms_orderSegmentsByMinAge = true;
        ms_orderSegmentsBySegmentAge = false;
        ms_takeRootOfAge = false;
        ms_takeLogOfAge = false;
        ms_sortSurvivorObjects = true;
        ms_resetObjectTimestampsWhenCleaning = false;
        ms_useRamcloudCostBenefit = false;
        ms_useDxramCostBenefit = false;
        ms_useDecayForCostBenefit = false;
        ms_useLifetimesWhenReplaying = false;
        ms_rootExp = 0.5;
        ms_logBase = 2;

        ms_segmentsCleaned = 0;
        ms_cleaningPasses = 0;
        ms_segmentsFreeAfterCleaning = 0;
        ms_emptySegmentsCleaned = 0;
        ms_newObjectsWritten = 0;
        ms_cleanerObjectsWritten = 0;
        ms_currentTimestamp = 0;
        ms_objectWriteCounts = new int[TOTAL_LIVE_OBJECTS];

        ms_utilisation = 75;
        ms_line = 0;
        ms_lastN = -1;
        ms_lastS = 0;
        ms_cachedResult = 0;
        ms_generated = false;
        ms_fillIds = new ArrayList<Integer>();
        ms_random = new Random(System.nanoTime());

        ms_preCleaningUtilisations = new Utilisations();
        ms_cleanedSegmentUtilisations = new Utilisations();

        // Parse program arguments
        while (i < p_arguments.length) {
            switch (p_arguments[i]) {
                case "-a":
                    ms_orderSegmentsByMinAge = false;
                    ms_orderSegmentsBySegmentAge = false;
                    break;
                case "-A":
                    ms_orderSegmentsBySegmentAge = true;
                    ms_orderSegmentsByMinAge = false;
                    break;
                case "-d":
                    if (p_arguments[i + 1].equals("zipfian")) {
                        ms_distribution = new ZipfianDistribution(TOTAL_LIVE_OBJECTS);
                    } else {
                        ms_distribution = new Distribution(p_arguments[i + 1]);
                    }
                    i++;
                    break;
                case "-D":
                    ms_useDecayForCostBenefit = true;
                    break;
                case "-o":
                    try {
                        outScriptFile = new FileWriter(p_arguments[i + 1]);
                    } catch (final IOException e) {
                        System.out.println("failed to open output script file");
                        System.exit(1);
                    }
                    i++;
                    break;
                case "-i":
                    try {
                        inScriptFile = new FileReader(p_arguments[i + 1]);
                    } catch (final FileNotFoundException e) {
                        System.out.println("failed to open input script file");
                        System.exit(1);
                    }
                    i++;
                    break;
                case "-l":
                    ms_useLifetimesWhenReplaying = true;
                    break;
                case "-L":
                    ms_takeLogOfAge = true;
                    if (p_arguments[i + 1].equalsIgnoreCase("e")) {
                        ms_logBase = Math.E;
                    } else {
                        ms_logBase = Integer.parseInt(p_arguments[i + 1]);
                    }
                    i++;
                    break;
                case "-r":
                    ms_sortSurvivorObjects = false;
                    break;
                case "-f":
                    if (p_arguments[i + 1].equals("rc")) {
                        ms_useRamcloudCostBenefit = true;
                    } else if (p_arguments[i + 1].equals("dx")) {
                        ms_useDxramCostBenefit = true;
                    }
                    i++;
                    break;
                case "-s":
                    ms_strategy = new Strategy(p_arguments[i + 1]);
                    i++;
                    break;
                case "-S":
                    ms_takeRootOfAge = true;
                    ms_rootExp = Double.parseDouble(p_arguments[i + 1]);
                    i++;
                    break;
                case "-t":
                    ms_resetObjectTimestampsWhenCleaning = true;
                    break;
                case "-u":
                    ms_utilisation = Integer.parseInt(p_arguments[i + 1]);
                    if (ms_utilisation < 1 || ms_utilisation > 99) {
                        usage();
                    }
                    i++;
                    break;
                default:
                    usage();
                    break;
            }
            i++;
        }

        if (ms_takeRootOfAge && ms_takeLogOfAge) {
            System.out.println("error: only one of -L or -S may be passed");
            usage();
        }

        if (ms_useLifetimesWhenReplaying && inScriptFile == null) {
            System.out.println("error: cannot use -l flag without -i as well");
            usage();
        }

        if (ms_distribution == null) {
            ms_distribution = new Distribution("hotAndCold");
        }
        if (ms_strategy == null) {
            ms_strategy = new Strategy("costBenefit");
        }

        System.out.println("########### Parameters ###########");
        System.out.format("# DISTRIBUTON = %s\n", ms_distribution.toString());
        System.out.format("# STRATEGY = %s\n", ms_strategy.toString());
        System.out.format("# UTILISATION = %d\n", ms_utilisation);
        System.out.format("# OBJECTS_PER_SEGMENT = %d\n", OBJECTS_PER_SEGMENT);
        System.out.format("# TOTAL_LIVE_OBJECTS = %d\n", TOTAL_LIVE_OBJECTS);
        System.out.format("# MAX_LIVE_OBJECTS_WRITTEN_PER_PASS = %d\n", MAX_LIVE_OBJECTS_WRITTEN_PER_PASS);
        if (ms_orderSegmentsByMinAge) {
            System.out.println("# ORDER_SEGMENTS_BY_MIN_AGE = true");
        } else {
            System.out.println("# ORDER_SEGMENTS_BY_MIN_AGE = false");
        }
        if (ms_orderSegmentsBySegmentAge) {
            System.out.println("# ORDER_SEGMENTS_BY_SEGMENT_AGE = true");
        } else {
            System.out.println("# ORDER_SEGMENTS_BY_SEGMENT_AGE = false");
        }
        if (ms_takeRootOfAge) {
            System.out.format("# TAKE_ROOT_OF_AGE = true (exp = %.5f)\n", ms_rootExp);
        } else {
            System.out.format("# TAKE_ROOT_OF_AGE = false (exp = %.5f)\n", ms_rootExp);
        }
        if (ms_takeLogOfAge) {
            System.out.format("# TAKE_LOG_OF_AGE = true (base = %.5f)\n", ms_logBase);
        } else {
            System.out.format("# TAKE_LOG_OF_AGE = false (base = %.5f)\n", ms_logBase);
        }
        if (ms_sortSurvivorObjects) {
            System.out.println("# SORT_SURVIVOR_OBJECTS = true");
        } else {
            System.out.println("# SORT_SURVIVOR_OBJECTS = false");
        }
        if (ms_resetObjectTimestampsWhenCleaning) {
            System.out.println("# RESET_OBJECT_TIMESTAMPS_WHEN_CLEANING = true");
        } else {
            System.out.println("# RESET_OBJECT_TIMESTAMPS_WHEN_CLEANING = false");
        }
        if (ms_useRamcloudCostBenefit) {
            System.out.println("# COST_BENEFIT_FUNCTION = RAMCloud");
        } else if (ms_useDxramCostBenefit) {
            System.out.println("# COST_BENEFIT_FUNCTION = DxRAM");
        } else {
            System.out.println("# COST_BENEFIT_FUNCTION = DEFAULT");
        }
        if (ms_useDecayForCostBenefit) {
            System.out.println("# USE_DECAY_FOR_COST_BENEFIT = true");
        } else {
            System.out.println("# USE_DECAY_FOR_COST_BENEFIT = false");
        }
        if (ms_useLifetimesWhenReplaying) {
            System.out.println("# USE_LIFETIMES_WHEN_REPLAYING = true");
        } else {
            System.out.println("# USE_LIFETIMES_WHEN_REPLAYING = false");
        }
        System.out.println("##################################");

        lifetime = -1;
        liveDataSegments = TOTAL_LIVE_OBJECTS / OBJECTS_PER_SEGMENT;
        totalSegments = (int) (100.0 / ms_utilisation * liveDataSegments);

        System.out.format("# Total Segments: %d, Live Data Segments: %d\n", totalSegments, liveDataSegments);
        System.out.format("# Desired u = %.2f, actual u = %.2f\n", ms_utilisation / 100.0, (double) liveDataSegments / totalSegments);

        for (i = 0; i < totalSegments; i++) {
            freeList.add(new Segment());
        }
        for (i = 0; i < TOTAL_LIVE_OBJECTS; i++) {
            objectToSegment.add(new ObjectReference(null, -1));
        }

        lastWc = 0;
        writesSinceChange = 0;
        lastPrintTime = 0;

        startTime = System.currentTimeMillis();
        while (true) {
            if (head != null && head.full()) {
                activeList.add(head);
                head = null;
            }

            if (head == null) {
                if (freeList.isEmpty()) {
                    head = clean(activeList, freeList, objectToSegment);
                    // It's possible that the head is full, so just continue.
                    continue;
                } else {
                    head = freeList.remove(freeList.size() - 1);
                }
            }

            retValues = nextObject(inScriptFile);
            obj = retValues[0];
            lifetime = retValues[1];
            if (obj == -1) {
                break;
            }

            if (objectToSegment.get(obj).getSegment() != null) {
                objectToSegment.get(obj).getSegment().free(objectToSegment.get(obj));
            }
            objectToSegment.set(obj, head.append(obj, ms_currentTimestamp++, lifetime));
            ms_objectWriteCounts[obj]++;
            ms_newObjectsWritten++;

            if (outScriptFile != null) {
                writes.add(obj);
            }

            // Detect and quit when u stabilizes.
            wcLSF = lsfWriteCost();
            wcRC = ramcloudWriteCost();
            if (Math.abs(wcLSF - lastWc) >= 0.0001) {
                lastWc = wcLSF;
                writesSinceChange = 0;
            }

            writesSinceChange++;
            if (writesSinceChange == 2 * totalSegments * OBJECTS_PER_SEGMENT && inScriptFile == null) {
                break;
            }

            // Print out every ~1 second. Don't check every time to cut overhead.
            if (ms_newObjectsWritten % 99991 == 0) {
                runTime = Math.max(1, (System.currentTimeMillis() - startTime) / 1000);
                if (runTime > lastPrintTime) {
                    System.out.format("\r Wrote %d new objects (%d / sec), Cleaned %d segments (%d empty)," + "%d survivor objects, LSFwc = %.3f, RCwc = %.3f",
                            ms_newObjectsWritten, ms_newObjectsWritten / runTime, ms_segmentsCleaned, ms_emptySegmentsCleaned, ms_cleanerObjectsWritten, wcLSF,
                            wcRC);
                    System.out.flush();
                    lastPrintTime = runTime;
                }
            }

            assert activeList.size() + freeList.size() + 1 == totalSegments;
        }

        System.out.println("\nDone!");

        System.out.format("# Total simulation time = %d seconds\n", (int) ((System.currentTimeMillis() - startTime) / 1000));
        System.out.format("# New object writes = %d\n", ms_newObjectsWritten);
        System.out.format("# Survivor objects written by cleaner = %d\n", ms_cleanerObjectsWritten);
        System.out.format("# LSF write cost = %.3f\n", lsfWriteCost());
        System.out.format("# Cleaning passes = %d\n", ms_cleaningPasses);
        System.out.format("# Segments cleaned = %d\n", ms_segmentsCleaned);
        System.out.format("# Average segments cleaned per pass = %.2f\n", (double) ms_segmentsCleaned / ms_cleaningPasses);
        System.out.format("# Average segments free after cleaning = %.2f\n", (double) ms_segmentsFreeAfterCleaning / ms_cleaningPasses);
        System.out.format("# Objects read from cleaned segments = %d\n", ms_segmentsCleaned * OBJECTS_PER_SEGMENT);

        ms_preCleaningUtilisations.dump("Live Segment Utilisations Prior to Cleaning");
        // So we can use the gnuplot 'index' to choose between them
        System.out.println("\n");
        ms_cleanedSegmentUtilisations.dump("Cleaned Segment Utilisations");
        System.out.println("\n");
        dumpObjectWriteHistogram();

        if (outScriptFile != null) {
            generateScriptFile(writes, outScriptFile);
        }
    }

    // Classes

    /**
     * ObjectReference
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    public static final class ObjectReference {

        // Attributes
        // Segment containing the object.
        private Segment m_segment;
        // Offset of the object within the segment.
        private int m_index;

        // Constructors

        /**
         * Creates an instance of ObjectReference
         *
         * @param p_objectSegment
         *         ...
         * @param p_objectIndex
         *         ...
         */
        public ObjectReference(final Segment p_objectSegment, final int p_objectIndex) {
            m_segment = p_objectSegment;
            m_index = p_objectIndex;
        }

        // Getter

        /**
         * ...
         *
         * @return ...
         */
        public Segment getSegment() {
            return m_segment;
        }
    }

    /**
     * Obj
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    public static final class Obj {

        // Attributes
        // Unique identifier of the object.
        private int m_id;
        // Simulation time at which the object was written.
        private long m_timestamp;
        // When the object will be overwritten (die). This only makes sense if
        // you're replaying a script from a previous run, or if your computer
        // happens to be an oracle.
        private long m_timeOfDeath;

        // Constructors

        /**
         * Creates an instance of Obj
         *
         * @param p_objectId
         *         ...
         * @param p_objectTimestamp
         *         ...
         * @param p_objectTimeOfDeath
         *         ...
         */
        public Obj(final int p_objectId, final long p_objectTimestamp, final long p_objectTimeOfDeath) {
            m_id = p_objectId;
            m_timestamp = p_objectTimestamp;
            m_timeOfDeath = p_objectTimeOfDeath;
        }

        // Methods

        /**
         * ...
         *
         * @return ...
         */
        public boolean isDead() {
            return m_id == DEAD_OBJECT_ID;
        }

        /**
         * ...
         *
         * @return ...
         */
        public int getId() {
            return m_id;
        }

        /**
         * ...
         *
         * @return ...
         */
        public long getTimestamp() {
            return m_timestamp;
        }

        /**
         * ...
         *
         * @return ...
         */
        public long getTimeOfDeath() {
            return m_timeOfDeath;
        }
    }

    /**
     * Segment
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    public static final class Segment {

        // Attributes
        // Log order vector of objects written.
        private ArrayList<Obj> m_objects;
        // Number of objects currently alive in this segment.
        private int m_liveObjectCount;
        // Sum of the timestamps of all objects appended to this segment.
        private long m_timestampSum;
        // Timestamp of the the youngest object appended to this segment.
        private long m_latestTimestamp;
        // Timestamp when this segment was created.
        private long m_createTimestamp;

        // Constructors

        /**
         * Creates an instance of Segment
         */
        public Segment() {
            m_objects = new ArrayList<Obj>();
            m_liveObjectCount = 0;
            m_timestampSum = 0;
            m_latestTimestamp = 0;
            m_createTimestamp = ms_currentTimestamp;
        }

        // Methods

        /**
         * ...
         *
         * @param p_objectId
         *         ...
         * @param p_timestamp
         *         ...
         * @param p_timeOfDeath
         *         ...
         * @return ...
         */
        public ObjectReference append(final int p_objectId, final long p_timestamp, final long p_timeOfDeath) {
            assert m_objects.size() < OBJECTS_PER_SEGMENT;

            m_timestampSum += p_timestamp;
            m_latestTimestamp = Math.max(m_latestTimestamp, p_timestamp);
            m_objects.add(new Obj(p_objectId, p_timestamp, p_timeOfDeath));
            m_liveObjectCount++;

            return new ObjectReference(this, m_objects.size() - 1);
        }

        /**
         * ...
         *
         * @param p_ref
         *         ...
         */
        public void free(final ObjectReference p_ref) {
            long timestamp;

            assert !m_objects.get(p_ref.m_index).isDead();

            timestamp = m_objects.get(p_ref.m_index).getTimestamp();
            m_timestampSum -= timestamp;
            m_objects.set(p_ref.m_index, new Obj(DEAD_OBJECT_ID, 0, 0));

            // If this was our youngest object, then linearly search for the next youngest.
            // This occurs infrequently, so it's probably not worth optimizing.
            //
            // I don't think Sprite LSF did this, and I'm not sure about Mendel's simulator.
            // In any event, it doesn't appear to make a big difference. That seems sensible,
            // since we'll segregate objects by age and the next live minimum is probably close
            // to the previous minimum.
            if (timestamp == m_latestTimestamp) {
                m_latestTimestamp = 0;
                for (int i = 0; i < m_objects.size(); i++) {
                    if (m_objects.get(i).isDead()) {
                        continue;
                    }
                    m_latestTimestamp = Math.max(m_latestTimestamp, m_objects.get(i).getTimestamp());
                }
            }

            m_liveObjectCount--;
        }

        /**
         * ...
         *
         * @return ...
         */
        public int size() {
            return m_objects.size();
        }

        /**
         * ...
         *
         * @param p_index
         *         ...
         * @return ...
         */
        public Obj operator(final int p_index) {
            assert p_index < size();

            return m_objects.get(p_index);
        }

        /**
         * ...
         *
         * @return ...
         */
        public double utilisation() {
            return (double) m_liveObjectCount / OBJECTS_PER_SEGMENT;
        }

        /**
         * ...
         *
         * @return ...
         */
        public boolean full() {
            return m_objects.size() == OBJECTS_PER_SEGMENT;
        }

        /**
         * ...
         *
         * @return ...
         */
        public int liveObjects() {
            return m_liveObjectCount;
        }

        /**
         * ...
         *
         * @return ...
         */
        public long getTimestamp() {
            long ret;

            if (m_liveObjectCount == 0) {
                ret = 0;
            } else if (ms_orderSegmentsByMinAge) {
                ret = m_latestTimestamp;
            } else {
                ret = (int) (m_timestampSum / m_liveObjectCount);
            }

            return ret;
        }

        /**
         * ...
         *
         * @return ...
         */
        public long getCreationTimestamp() {
            return m_createTimestamp;
        }
    }

    /**
     * Distribution
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    public static class Distribution {

        // Attributes
        private String m_name;
        private NextObjectMethod m_nextObjectMethod;

        // Constructors

        /**
         * Creates an instance of Distribution
         *
         * @param p_distributionName
         *         ...
         */
        public Distribution(final String p_distributionName) {
            m_name = p_distributionName;

            if (m_name.equals("hotAndCold")) {
                m_nextObjectMethod = new NextObjectMethod() {
                    @Override
                    public int nextObject() {
                        int ret;
                        int lastHotObjectId;
                        final int hotDataSpacePct = 10;
                        final int hotDataAccessPct = 90;
                        double hotFraction;

                        hotFraction = hotDataSpacePct / 100.0;
                        lastHotObjectId = (int) (hotFraction * TOTAL_LIVE_OBJECTS);

                        if (randInt(0, 99) < hotDataAccessPct) {
                            ret = (int) randInt(0, lastHotObjectId - 1);
                        } else {
                            ret = (int) randInt(lastHotObjectId, TOTAL_LIVE_OBJECTS - 1);
                        }

                        return ret;
                    }
                };
            } else if (m_name.equals("exponential")) {
                m_nextObjectMethod = new NextObjectMethod() {
                    @Override
                    public int nextObject() {
                        // Mendel's value. Supposedly 90% of writes go to 2% of data.
                        // In order for that to happen we need to cap the range of
                        // our random variants to be in [0, 2.5), essentially shifting
                        // our exponential curve to the left.
                        final double mu = 0.02172;
                        final double cap = 2.5;
                        // Note that mu = 1/lambda
                        double expRnd;

                        do {
                            expRnd = -Math.log(uniformUnit()) * mu;
                        } while (expRnd >= cap);

                        // Normalize to [0, 1) and choose the corresponding object
                        // (each one corresponds to an equal-sized slice in that range).
                        return (int) (expRnd / cap * TOTAL_LIVE_OBJECTS);
                    }
                };
            } else if (m_name.equals("uniform")) {
                m_nextObjectMethod = new NextObjectMethod() {
                    @Override
                    public int nextObject() {
                        return (int) randInt(0, TOTAL_LIVE_OBJECTS - 1);
                    }
                };
            } else if (!m_name.equals("zipfian")) {
                System.exit(-1);
            }
        }

        // Methods

        /**
         * Returns a number selected uniform randomly in the unit interval (0, 1)
         *
         * @return ...
         */
        static double uniformUnit() {
            double ret;

            do {
                ret = ms_random.nextDouble();
                assert ret >= 0 && ret <= 1;
            } while (ret == 0 || ret == 1);

            return ret;
        }

        /**
         * ...
         *
         * @param p_min
         *         ...
         * @param p_max
         *         ...
         * @return ...
         */
        protected static long randInt(final long p_min, final long p_max) {
            long ret;

            assert p_max >= p_min;

            ret = p_min + ms_random.nextLong() % (p_max - p_min + 1);

            if (ret < 0) {
                ret *= -1;
            }
            return ret;
        }

        /**
         * ...
         *
         * @return ...
         */
        public int getNextObject() {
            int n;

            n = m_nextObjectMethod.nextObject();
            assert n >= 0 && n < TOTAL_LIVE_OBJECTS;

            return n;
        }

        @Override
        public final String toString() {
            return m_name;
        }
    }

    /**
     * Distribution
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    public static final class ZipfianDistribution extends Distribution {

        // Attributes
        private int m_totalObjects;
        private ArrayList<Group> m_groupsTable;

        // Constructors

        /**
         * Creates an instance of ZipfianDistribution
         *
         * @param p_totalObjects
         *         ...
         */
        public ZipfianDistribution(final int p_totalObjects) {
            super("zipfian");

            double s;

            m_totalObjects = p_totalObjects;
            assert m_totalObjects > 0;
            m_groupsTable = new ArrayList<Group>();

            System.out.println("ZipfianDistribution: Calculating skew value (may take a while)...");
            s = calculateSkew(m_totalObjects - 1, 90, 15);
            System.out.println("ZipfianDistribution: done.");
            generateTable(m_totalObjects - 1, s);
        }

        // Methods

        /**
         * ...
         *
         * @param p_n
         *         ...
         * @param p_s
         *         ...
         * @return ...
         */
        private static double generalizedHarmonic(final long p_n, final double p_s) {

            if (p_n != ms_lastN || p_s != ms_lastS) {
                ms_lastN = p_n;
                ms_lastS = p_s;
                ms_cachedResult = 0;
                for (long n = 1; n <= p_n; n++) {
                    ms_cachedResult += 1.0 / Math.pow(n, p_s);
                }
            }

            return ms_cachedResult;
        }

        /* Zipfian frequency formula from wikipedia. */

        /**
         * ...
         *
         * @param p_k
         *         ...
         * @param p_s
         *         ...
         * @param p_n
         *         ...
         * @return ...
         */
        public static double f(final long p_k, final double p_s, final long p_n) {
            return 1.0 / Math.pow(p_k, p_s) / generalizedHarmonic(p_n, p_s);
        }

        /**
         * ...
         *
         * @param p_n
         *         ...
         * @param p_s
         *         ...
         * @param p_hotAccessPct
         *         ...
         * @return ...
         */
        public static double getHotDataPct(final long p_n, final double p_s, final int p_hotAccessPct) {
            double ret = 100;
            double sum = 0;
            double p;

            for (long i = 1; i <= p_n; i++) {
                p = f(i, p_s, p_n);
                sum += 100.0 * p;
                if (sum >= p_hotAccessPct) {
                    ret = 100.0 * i / p_n;
                    break;
                }
            }
            return ret;
        }

        /**
         * Try to compute the proper skew value to get a Zipfian distribution
         * over N keys where hotAccessPct of the requests go to hotDataPct of
         * the data.
         * This method simply does a binary search across a range of skew
         * values until it finds something close. For large values of N this
         * can take a while (a few minutes when N = 100e6). If there's a fast
         * approximation of the generalizedHarmonic method above (which supposedly
         * converges to the Riemann Zeta Function for large N), this could be
         * significantly improved.
         * But, it's good enough for current purposes.
         *
         * @param p_n
         *         ...
         * @param p_hotAccessPct
         *         ...
         * @param p_hotDataPct
         *         ...
         * @return ...
         */
        public static double calculateSkew(final long p_n, final int p_hotAccessPct, final int p_hotDataPct) {
            double ret = 0;
            double minS = 0.01;
            double maxS = 10;
            double s;
            double r;
            boolean hit;

            hit = false;
            for (int i = 0; i < 100; i++) {
                s = (minS + maxS) / 2;
                r = getHotDataPct(p_n, s, p_hotAccessPct);
                if (Math.abs(r - p_hotDataPct) < 0.5) {
                    ret = s;
                    hit = true;
                    break;
                }
                if (r > p_hotDataPct) {
                    minS = s;
                } else {
                    maxS = s;
                }
            }

            if (!hit) {
                // Bummer. We didn't find a good value. Just bail.
                System.out.format("calculateSkew: Failed to calculate reasonable skew for" + " N = %d, %d%% -> %d%%\n", p_n, p_hotAccessPct, p_hotDataPct);
                System.exit(1);
            }
            return ret;
        }

        /**
         * It'd require too much space and be too slow to generate a full histogram
         * representing the frequency of each individual key when there are a large
         * number of them. Instead, we divide the keys into a number of groups and
         * choose a group based on the cumulative frequency of keys it contains. We
         * then uniform-randomly select a key within the chosen group (see the
         * chooseNextKey method).
         * The number of groups is fixed (1000) and each contains 0.1% of the key
         * frequency space. This means that groups tend to increase exponentially in
         * the size of the range of keys they contain (unless the Zipfian distribution
         * was skewed all the way to uniform). The result is that we are very accurate
         * for the most popular keys (the most popular keys are singleton groups) and
         * only lose accuracy for ones further out in the curve where it's much flatter
         * anyway.
         *
         * @param p_n
         *         ...
         * @param p_s
         *         ...
         */
        public void generateTable(final long p_n, final double p_s) {
            final double groupFrequencySpan = 0.001;
            double p;
            double cdf = 0;
            double startCdf = 0;
            long startI = 1;

            for (long i = startI; i <= p_n; i++) {
                p = f(i, p_s, p_n);
                cdf += p;
                if (cdf - startCdf >= groupFrequencySpan || i == p_n) {
                    m_groupsTable.add(new Group(startI, i, startCdf));
                    startCdf = cdf;
                    startI = i + 1;
                }
            }
        }

        /**
         * Obtain the next key fitting the Zipfian distribution described by the
         * 'groupsTable' table.
         * This method binary searches (logn complexity), but the groups table
         * is small enough to fit in L2 cache, so it's quite fast (~6M keys
         * per second -- more than sufficient for now).
         *
         * @return ...
         */
        public long chooseNextKey() {
            long ret;
            int i;
            long min = 0;
            long max = m_groupsTable.size() - 1;
            double p;
            double start;
            double end;

            p = uniformUnit();
            while (true) {
                i = (int) ((min + max) / 2);
                start = m_groupsTable.get(i).getFirstCdf();
                end = 1.1;
                if (i != m_groupsTable.size() - 1) {
                    end = m_groupsTable.get(i + 1).getFirstCdf();
                }
                if (p < start) {
                    max = i - 1;
                } else if (p >= end) {
                    min = i + 1;
                } else {
                    ret = randInt(m_groupsTable.get(i).getFirstKey(), m_groupsTable.get(i).getLastKey());
                    return ret;
                }
            }
        }

        @Override
        public int getNextObject() {
            return (int) chooseNextKey();
        }
    }

    /**
     * Strategy
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    public static final class Strategy {

        // Attributes
        private String m_name;
        private ChooseSegmentMethod m_chooseSegmentMethod;

        // Constructors

        /**
         * Creates an instance of Strategy
         *
         * @param p_strategyName
         *         ...
         */
        public Strategy(final String p_strategyName) {
            m_name = p_strategyName;

            if (m_name.equals("costBenefit")) {
                m_chooseSegmentMethod = new ChooseSegmentMethod() {
                    @Override
                    public void chooseSegment(final ArrayList<Segment> p_activeList, final ArrayList<Segment> p_segmentsToClean) {

                        Collections.sort(p_activeList, new CostBenefitComparator());

                        int objectCount = 0;
                        while (!p_activeList.isEmpty()) {
                            objectCount += p_activeList.get(p_activeList.size() - 1).liveObjects();
                            if (objectCount > MAX_LIVE_OBJECTS_WRITTEN_PER_PASS) {
                                break;
                            }

                            p_segmentsToClean.add(p_activeList.remove(p_activeList.size() - 1));
                        }
                    }
                };
            } else if (m_name.equals("greedy")) {
                m_chooseSegmentMethod = new ChooseSegmentMethod() {
                    @Override
                    public void chooseSegment(final ArrayList<Segment> p_activeList, final ArrayList<Segment> p_segmentsToClean) {
                        int leastIdx;
                        int objectCount = 0;

                        while (true) {
                            if (p_activeList.size() == 0) {
                                break;
                            }

                            leastIdx = 0;
                            for (int i = 0; i < p_activeList.size(); i++) {
                                if (p_activeList.get(i).utilisation() < p_activeList.get(leastIdx).utilisation()) {
                                    leastIdx = i;
                                }
                            }

                            objectCount += p_activeList.get(leastIdx).liveObjects();
                            if (objectCount > MAX_LIVE_OBJECTS_WRITTEN_PER_PASS) {
                                break;
                            }

                            p_segmentsToClean.add(p_activeList.get(leastIdx));

                            // remove it from the activeList
                            if (p_activeList.size() - 1 > leastIdx) {
                                p_activeList.set(leastIdx, p_activeList.remove(p_activeList.size() - 1));
                            } else {
                                p_activeList.remove(p_activeList.size() - 1);
                            }
                        }
                    }
                };
            } else {
                System.exit(-1);
            }
        }

        @Override
        public String toString() {
            return m_name;
        }

        /**
         * ...
         *
         * @param p_activeList
         *         ...
         * @param p_segmentsToClean
         *         ...
         */
        public void chooseSegments(final ArrayList<Segment> p_activeList, final ArrayList<Segment> p_segmentsToClean) {
            m_chooseSegmentMethod.chooseSegment(p_activeList, p_segmentsToClean);
        }

        /**
         * Used to sort our list of active segments by cost-benefit score, from
         * lowest-scoring at the front to highest-scoring at the back.
         *
         * @author Kevin Beineke
         *         05.03.2014
         */
        public class CostBenefitComparator implements Comparator<Segment> {

            // Constructors

            /**
             * Creates an instance of CostBenefitComparator
             */
            public CostBenefitComparator() {
            }

            // Methods

            /**
             * ...
             *
             * @param p_segment
             *         ...
             * @return ...
             */
            public final double costBenefit(final Segment p_segment) {
                double ret;
                long segmentAge;
                long age;
                double u;
                double decay;

                u = p_segment.utilisation();
                if (u == 0) {
                    ret = Double.MAX_VALUE;
                } else if (u == 1) {
                    ret = Double.MIN_VALUE;
                } else {
                    segmentAge = ms_currentTimestamp - p_segment.getCreationTimestamp();
                    if (ms_useDecayForCostBenefit) {
                        segmentAge = Math.max(1, segmentAge);
                        decay = (1 - u) / segmentAge;
                        ret = (1.0 - u) / ((1 + u) * Math.pow(decay, ms_rootExp));
                    } else {
                        age = ms_currentTimestamp - p_segment.getTimestamp();
                        if (ms_orderSegmentsBySegmentAge) {
                            age = segmentAge;
                        }

                        if (ms_takeRootOfAge) {
                            age = (long) Math.pow(age, ms_rootExp);
                        } else if (ms_takeLogOfAge) {
                            age = (long) (Math.log(age) / Math.log(ms_logBase));
                        }

                        if (ms_useRamcloudCostBenefit) {
                            ret = (1.0 - u) * age / u;
                        } else if (ms_useDxramCostBenefit) {
                            // TODO: Adapt cost benefit function
                            ret = (1.0 - u) * age / u;
                        } else {
                            // Default LSF cost-benefit formula.
                            ret = (1.0 - u) * age / (1.0 + u);
                        }
                    }
                }
                return ret;
            }

            @Override
            public final int compare(final Segment p_a, final Segment p_b) {
                int ret = 0;
                double diff;

                diff = costBenefit(p_a) - costBenefit(p_b);
                if (diff > 0) {
                    ret = 1;
                } else if (diff < 0) {
                    ret = -1;
                }
                return ret;
            }
        }
    }

    /**
     * Utilisation
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    public static final class Utilisations {

        // Attributes
        private int m_totalSamples;
        private double m_total;
        private int[] m_counts;
        private int m_buckets = 1000;

        // Constructors

        // Methods

        /**
         * Creates an instance of Utilisation
         */
        public Utilisations() {
            m_totalSamples = 0;
            m_counts = new int[m_buckets + 1];
        }

        /**
         * ...
         *
         * @param p_u
         *         ...
         */
        public void store(final double p_u) {
            m_counts[(int) Math.round(m_buckets * p_u)]++;
            m_totalSamples++;
            m_total += p_u;
        }

        /**
         * ...
         *
         * @param p_description
         *         ...
         */
        public void dump(final String p_description) {
            System.out.format("####\n# %s (avg: %.3f)\n####\n", p_description, m_total / m_totalSamples);

            long sum = 0;
            for (int i = 0; i <= m_buckets; i++) {
                if (m_counts[i] == 0) {
                    continue;
                }
                sum += m_counts[i];
                System.out.format("%f %f %f\n", (double) i / m_buckets, (double) m_counts[i] / m_totalSamples, (double) sum / m_totalSamples);
            }
        }
    }

    /**
     * Group
     * Each of these entries represents a contiguous portion of the CDF curve
     * of keys vs. frequency. We use these to sample keys out to fit an
     * approximation of the Zipfian distribution. See generateTable for more
     * details on what's going on here.
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    static class Group {

        // Attributes
        private long m_firstKey;
        private long m_lastKey;
        private double m_firstCdf;

        /**
         * Creates an instance of Group
         *
         * @param p_firstKey
         *         ...
         * @param p_lastKey
         *         ...
         * @param p_firstCdf
         *         ...
         */
        Group(final long p_firstKey, final long p_lastKey, final double p_firstCdf) {
            m_firstKey = p_firstKey;
            m_lastKey = p_lastKey;
            m_firstCdf = p_firstCdf;
        }

        // Getter

        /**
         * ...
         *
         * @return ...
         */
        public long getFirstKey() {
            return m_firstKey;
        }

        /**
         * ...
         *
         * @return ...
         */
        public long getLastKey() {
            return m_lastKey;
        }

        /**
         * ...
         *
         * @return ...
         */
        public double getFirstCdf() {
            return m_firstCdf;
        }
    }

    /**
     * Pair
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    static class Pair {

        // Attributes
        private int m_firstValue;
        private int m_secondValue;

        /**
         * Creates an instance of Group
         *
         * @param p_firstValue
         *         ...
         * @param p_secondValue
         *         ...
         */
        Pair(final int p_firstValue, final int p_secondValue) {
            m_firstValue = p_firstValue;
            m_secondValue = p_secondValue;
        }

        // Getter

        /**
         * ...
         *
         * @return ...
         */
        public int getFirstValue() {
            return m_firstValue;
        }

        /**
         * ...
         *
         * @return ...
         */
        public int getSecondValue() {
            return m_secondValue;
        }

        // Setter

        /**
         * ...
         *
         * @param p_firstValue
         *         ...
         */
        public void setFirstValue(final int p_firstValue) {
            m_firstValue = p_firstValue;
        }

        /**
         * ...
         *
         * @param p_secondValue
         *         ...
         */
        public void setSecondValue(final int p_secondValue) {
            m_secondValue = p_secondValue;
        }
    }

    /**
     * Used to sort survivors such that the oldest are written first. This is slightly
     * preferable to writing oldest last because the last survivor segment is likely to
     * not be completely full and will mix in new writes. We'd prefer to mix hotter
     * data with the new writes (most of which will be hot) than with colder data.
     */
    static class TimeStampComparator implements Comparator<Obj> {

        // Constructors

        /**
         * Creates an instance of TimeStampComparator
         */
        TimeStampComparator() {
        }

        // Methods
        @Override
        public final int compare(final Obj p_a, final Obj p_b) {
            return (int) (p_a.getTimestamp() - p_b.getTimestamp());
        }
    }

    /**
     * Used to sort survivors when we're replaying from a script and using future
     * knowledge to our benefit. This sorts objects such that the ones to be
     * rewritten farthest in the future are written out first.
     */
    static class LifetimeComparator implements Comparator<Obj> {

        // Constructors

        /**
         * Creates an instance of LifetimeComparator
         */
        LifetimeComparator() {
        }

        // Methods
        @Override
        public final int compare(final Obj p_a, final Obj p_b) {
            return (int) (p_a.getTimeOfDeath() - p_b.getTimeOfDeath());
        }
    }

    /**
     * NextObjectMethod
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    interface NextObjectMethod {

        /**
         * ...
         *
         * @return ...
         */
        int nextObject();
    }

    /**
     * ChooseSegmentMethod
     *
     * @author Kevin Beineke
     *         05.03.2014
     */
    interface ChooseSegmentMethod {

        /**
         * ...
         *
         * @param p_activeList
         *         ...
         * @param p_segmentsToClean
         *         ...
         */
        void chooseSegment(ArrayList<Segment> p_activeList, ArrayList<Segment> p_segmentsToClean);
    }

}
