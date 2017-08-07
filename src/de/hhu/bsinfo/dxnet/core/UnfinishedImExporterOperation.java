package de.hhu.bsinfo.dxnet.core;

/**
 * Wrapper for caching an unfinished operation during import/export.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class UnfinishedImExporterOperation {
    private int m_startIndex;
    private long m_primitive;
    private Object m_object;

    /**
     * Creates an instance of UnfinishedImExporterOperation
     */
    UnfinishedImExporterOperation() {
    }

    /**
     * Get index within buffer of unfinished operation
     *
     * @return the index
     */
    int getIndex() {
        return m_startIndex;
    }

    /**
     * Get unfinished primitive (short, int, long, float or double)
     *
     * @return the primitive as long
     */
    long getPrimitive() {
        return m_primitive;
    }

    /**
     * Get unfinished object (e.g. byte array)
     *
     * @return
     */
    Object getObject() {
        return m_object;
    }

    /**
     * Set index of unfinished operation
     *
     * @param p_index
     *         the index
     */
    void setIndex(final int p_index) {
        m_startIndex = p_index;
    }

    /**
     * Set unfinished primitive
     *
     * @param p_primitive
     *         the primitive (short, int, long, float or double)
     */
    void setPrimitive(final long p_primitive) {
        m_primitive = p_primitive;
    }

    /**
     * Set unfinished object
     *
     * @param p_object
     *         the object
     */
    void setObject(final Object p_object) {
        m_object = p_object;
    }

    /**
     * Reset instance
     */
    void reset() {
        m_primitive = 0;
        m_object = null;
        m_startIndex = 0;
    }
}
