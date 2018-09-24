package de.hhu.bsinfo.dxram.log.storage.logs.secondarylog;

/**
 * Segment header.
 *
 * @author Kevin Beineke 07.11.2014
 */
public final class SegmentHeader {

    private final int m_index;
    private volatile int m_usedBytes;
    private volatile int m_lastAccess;
    private volatile int m_averageAge;
    private volatile boolean m_reorgInCurrEon;

    /**
     * Creates an instance of SegmentHeader.
     *
     * @param p_usedBytes
     *         the number of used bytes
     * @param p_index
     *         the index within the log
     * @param p_currentTime
     *         the current time in seconds
     */
    SegmentHeader(final int p_index, final int p_usedBytes, final int p_currentTime) {
        m_index = p_index;
        m_usedBytes = p_usedBytes;
        m_lastAccess = p_currentTime;
        m_averageAge = 0;
        m_reorgInCurrEon = true;
    }

    /**
     * Returns the utilization.
     *
     * @param p_logSegmentSize
     *         the log segment size
     * @return the utilization
     */
    double getUtilization(final int p_logSegmentSize) {
        return (double) m_usedBytes / p_logSegmentSize;
    }

    /**
     * Returns the index.
     *
     * @return the index
     */
    int getIndex() {
        return m_index;
    }

    /**
     * Returns whether this segment is empty or not.
     *
     * @return true if segment is empty, false otherwise
     */
    public boolean isEmpty() {
        return m_usedBytes == 0;
    }

    /**
     * Returns number of used bytes.
     *
     * @return number of used bytes
     */
    int getUsedBytes() {
        return m_usedBytes;
    }

    /**
     * Returns number of used bytes.
     *
     * @param p_logSegmentSize
     *         the log segment size
     * @return number of used bytes
     */
    int getFreeBytes(final int p_logSegmentSize) {
        return p_logSegmentSize - m_usedBytes;
    }

    /**
     * Returns the last access to this segment.
     *
     * @return the timestamp
     */
    int getLastAccess() {
        return m_lastAccess;
    }

    /**
     * Returns the age of this segment.
     *
     * @param p_currentTime
     *         the current time in seconds
     * @return the age of this segment
     */
    int getAge(final int p_currentTime) {
        return m_averageAge + p_currentTime - m_lastAccess;
    }

    /**
     * Sets the age.
     *
     * @param p_newAge
     *         the new calculated age
     */
    void setAge(final int p_newAge) {
        m_averageAge = p_newAge;
    }

    /**
     * Returns whether this segment was reorganized in current eon.
     *
     * @return whether this segment was reorganized in current eon
     */
    boolean wasNotReorganized() {
        return !m_reorgInCurrEon;
    }

    /**
     * Updates the number of used bytes.
     *
     * @param p_writtenBytes
     *         the number of written bytes
     * @param p_currentTime
     *         the current time in seconds
     */
    void updateUsedBytes(final int p_writtenBytes, final int p_currentTime) {
        m_usedBytes += p_writtenBytes; /* only one thread (writer or reorg.) can be active on a secondary log */
        m_lastAccess = p_currentTime;
    }

    /**
     * Sets the reorganization status for current eon
     */
    void markSegmentAsReorganized() {
        m_reorgInCurrEon = true;
    }

    /**
     * Resets the reorganization status for new eon
     */
    void beginEon() {
        m_reorgInCurrEon = false;
    }

    /**
     * Resets the segment header.
     *
     * @param p_currentTime
     *         the current time in seconds
     */
    void reset(final int p_currentTime) {
        m_usedBytes = 0;
        m_lastAccess = p_currentTime;
        m_averageAge = 0;
    }
}
