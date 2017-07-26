package de.hhu.bsinfo.net.core;

/**
 * Created by nothaas on 7/19/17.
 */
class UnfinishedImporterOperation {
    private int m_startIndex;
    private long m_primitive;
    private Object m_object;

    UnfinishedImporterOperation() {

    }

    int getIndex() {
        return m_startIndex;
    }

    long getPrimitive() {
        return m_primitive;
    }

    Object getObject() {
        return m_object;
    }

    void setIndex(final int p_index) {
        m_startIndex = p_index;
    }

    void setPrimitive(final long p_primitive) {
        m_primitive = p_primitive;
    }

    void setObject(final Object p_object) {
        m_object = p_object;
    }

    void reset() {
        m_primitive = 0;
        m_object = null;
        m_startIndex = 0;
    }
}
