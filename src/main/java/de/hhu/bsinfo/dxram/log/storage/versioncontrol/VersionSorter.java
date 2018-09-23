package de.hhu.bsinfo.dxram.log.storage.versioncontrol;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.ArrayListLong;
import de.hhu.bsinfo.dxutils.hashtable.LongHashTable;

/**
 * Gathers, sorts and combines chunk IDs based on data structures used to determine versions during the recovery.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public final class VersionSorter {

    private static final int SORT_THRESHOLD = 100000;

    private static final Logger LOGGER = LogManager.getFormatterLogger(VersionSorter.class.getSimpleName());

    /**
     * Private constructor.
     */
    private VersionSorter() {
    }

    /**
     * Determines ChunkID ranges for recovery
     *
     * @param p_versionStorage
     *         all versions in array and hashtable
     * @param p_lowestCID
     *         the lowest ChunkID in versions array
     * @return all ChunkID ranges
     */
    public static long[] determineRanges(final TemporaryVersionsStorage p_versionStorage, final long p_lowestCID) {
        long[] ret;
        long[] localRanges = null;
        long[] otherRanges = null;

        if (p_versionStorage.getVersionsArray().size() > 0) {
            localRanges = getRanges(p_versionStorage.getVersionsArray(), p_lowestCID);
        }
        if (p_versionStorage.getVersionsHashTable().size() > 0) {
            StringBuilder stringBuilder =
                    new StringBuilder("Hash table contains ").append(p_versionStorage.getVersionsHashTable().size())
                            .append(" entries. ").append(ChunkID.toHexString(p_lowestCID)).append('\n');
            if (localRanges != null) {
                stringBuilder.append("Local ranges:");
                for (long chunkID : localRanges) {
                    stringBuilder.append(' ').append(ChunkID.toHexString(chunkID));
                }
            }
            otherRanges = getRanges(p_versionStorage.getVersionsHashTable());
            stringBuilder.append("\nOther ranges:");
            for (long chunkID : otherRanges) {
                stringBuilder.append(' ').append(ChunkID.toHexString(chunkID));
            }
            LOGGER.info(stringBuilder);
        }

        if (localRanges == null) {
            ret = otherRanges;
        } else if (otherRanges == null) {
            ret = localRanges;
        } else {
            ret = new long[localRanges.length + otherRanges.length];
            System.arraycopy(localRanges, 0, ret, 0, localRanges.length);
            System.arraycopy(otherRanges, 0, ret, localRanges.length, otherRanges.length);
        }

        return ret;
    }

    /**
     * Determines all ChunkID ranges in versions array
     *
     * @param p_versionArray
     *         the version array
     * @return all ChunkID ranges in versions array
     */
    private static long[] getRanges(final VersionArray p_versionArray, final long p_lowestCID) {
        int currentIndex;
        int index = 0;
        long currentCID;
        ArrayListLong ranges = new ArrayListLong();

        while (index < p_versionArray.capacity()) {
            if (p_versionArray.getVersion(index, 0) == Version.INVALID_VERSION) {
                index++;
                continue;
            }

            currentCID = index + p_lowestCID;
            ranges.add(currentCID);
            currentIndex = 1;

            while (index + currentIndex < p_versionArray.capacity() &&
                    p_versionArray.getVersion(index + currentIndex, 0) != Version.INVALID_VERSION) {
                currentIndex++;
            }
            ranges.add(currentCID + currentIndex - 1);
            index += currentIndex;
        }

        return Arrays.copyOfRange(ranges.getArray(), 0, ranges.getSize());
    }

    /**
     * Determines all ChunkID ranges in versions hash table.
     *
     * @param p_versionsHT
     *         the versions hash table
     * @return all ChunkID ranges in versions hash table
     */
    private static long[] getRanges(final LongHashTable p_versionsHT) {
        int currentIndex;
        int index = 0;
        long currentCID;
        long[] table = p_versionsHT.getTable();
        ArrayListLong ranges = new ArrayListLong();

        // Sort table
        if (p_versionsHT.size() < SORT_THRESHOLD) {
            // There are only a few elements in table -> for a nearly sorted table
            // insertion sort is much faster than quicksort
            insertionSort(table);
        } else {
            quickSort(table, table.length - 1);
        }

        while (index < table.length) {
            if (table[index] == 0) {
                index += 2;
                continue;
            }

            currentCID = table[index];
            ranges.add(currentCID);
            currentIndex = 2;

            while (index + currentIndex < table.length &&
                    table[index + currentIndex] == currentCID + currentIndex / 2) {
                currentIndex += 2;
            }
            ranges.add(currentCID + currentIndex - 1);
            index += currentIndex;
        }

        return Arrays.copyOfRange(ranges.getArray(), 0, ranges.getSize());
    }

    /**
     * Sorts the versions hash table with insertion sort; Used for a barely utilized hash table
     * as insertion sort is best for nearly sorted series
     *
     * @param p_table
     *         the array of the versions hash table
     */
    private static void insertionSort(long[] p_table) {
        for (int i = 0; i < p_table.length / 2; i++) {
            for (int j = i; j > 0 && p_table[j * 2] < p_table[(j - 1) * 2]; j--) {
                swap(p_table, j, j - 1);
            }
        }
    }

    /**
     * Sorts the versions hash table with quicksort (iterative!); Used for highly a utilized hash table
     *
     * @param p_table
     *         the array of the versions hash table
     * @param p_right
     *         the range index
     */
    private static void quickSort(long[] p_table, int p_right) {
        int left = 0;
        int right = p_right;

        int[] stack = new int[right - left + 1];
        int top = -1;

        stack[++top] = left;
        stack[++top] = right;

        while (top >= 0) {
            right = stack[top--];
            left = stack[top--];

            int pivot = partition(p_table, left, right);

            if (pivot - 1 > left) {
                stack[++top] = left;
                stack[++top] = pivot - 1;
            }

            if (pivot + 1 < right) {
                stack[++top] = pivot + 1;
                stack[++top] = right;
            }
        }
    }

    /**
     * Helper method for quicksort to partition the range
     *
     * @param p_table
     *         the array of the versions hashtable
     * @param p_left
     *         the index of the pivot element
     * @param p_right
     *         the range index
     * @return the partition index
     */
    private static int partition(long[] p_table, int p_left, int p_right) {
        long x = p_table[p_right * 2];
        int i = p_left - 1;

        for (int j = p_left; j <= p_right - 1; j++) {
            if (p_table[j * 2] <= x) {
                i++;
                swap(p_table, i, j);
            }
        }
        swap(p_table, i + 1, p_right);

        return i + 1;
    }

    /**
     * Helper method for quicksort and insertion sort to swap to elements
     *
     * @param p_table
     *         the array of the versions hash table
     * @param p_index1
     *         the first index
     * @param p_index2
     *         the second index
     */
    private static void swap(long[] p_table, int p_index1, int p_index2) {
        int index1 = p_index1 * 2;
        int index2 = p_index2 * 2;

        if (p_table[index1] == 0) {
            if (p_table[index2] != 0) {
                p_table[index1] = p_table[index2];
                p_table[index1 + 1] = p_table[index2 + 1];

                p_table[index2] = 0;
                p_table[index2 + 1] = 0;
            }
        } else if (p_table[index2] == 0) {
            p_table[index2] = p_table[index1];
            p_table[index2 + 1] = p_table[index1 + 1];

            p_table[index1] = 0;
            p_table[index1 + 1] = 0;
        } else {
            long temp1 = p_table[index1];
            long temp2 = p_table[index1 + 1];

            p_table[index1] = p_table[index2];
            p_table[index1 + 1] = p_table[index2 + 1];

            p_table[index2] = temp1;
            p_table[index2 + 1] = temp2;
        }
    }
}
