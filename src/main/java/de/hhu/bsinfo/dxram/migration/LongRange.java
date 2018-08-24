package de.hhu.bsinfo.dxram.migration;

import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class LongRange {

    private final long m_from;

    private final long m_to;

    public LongRange(long m_from, long m_to) {
        this.m_from = m_from;
        this.m_to = m_to;
    }

    public long getTo() {
        return m_to;
    }

    public long getFrom() {
        return m_from;
    }

    public int size() {
        return (int) (m_to - m_from);
    }

    public LongStream stream() {
        return LongStream.range(m_from, m_to);
    }

    public static String collectionToString(final Collection<LongRange> p_ranges) {
        return p_ranges.stream().map(LongRange::toString).collect(Collectors.joining(" , ", "{", "}"));
    }

    public static int collectionToSize(final Collection<LongRange> p_ranges) {
        return p_ranges.stream().mapToInt(LongRange::size).sum();
    }

    public static long[] collectionToArray(final Collection<LongRange> p_ranges) {
        return p_ranges.stream().flatMapToLong(range -> Arrays.stream(range.toArray())).toArray();
    }

    public long[] toArray() {
        return new long[]{m_from, m_to};
    }

    public static Collection<LongRange> collectionFromArray(final long[] p_ranges) {
        List<LongRange> ranges = new ArrayList<>();

        for (int i = 0; i < p_ranges.length - 1; i += 2) {
            ranges.add(new LongRange(p_ranges[i], p_ranges[i + 1]));
        }

        return ranges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LongRange that = (LongRange) o;
        return m_from == that.m_from &&
                m_to == that.m_to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_from, m_to);
    }

    @Override
    public String toString() {
        return String.format("[%X,%X]", m_from, m_to);
    }
}
