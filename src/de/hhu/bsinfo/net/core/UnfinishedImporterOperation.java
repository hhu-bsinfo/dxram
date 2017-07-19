package de.hhu.bsinfo.net.core;

/**
 * Created by nothaas on 7/19/17.
 */
public class UnfinishedImporterOperation {
    private int m_startIndex;
    private long m_primitive;
    private Object m_object;

    public UnfinishedImporterOperation() {

    }

    public int getIndex() {
        return m_startIndex;
    }

    public long getPrimitive() {
        return m_primitive;
    }

    public Object getObject() {
        return m_object;
    }

    public void setIndex(final int p_index) {
        m_startIndex = p_index;
    }

    public void setPrimitive(final long p_primitive) {
        m_primitive = p_primitive;
    }

    public void setObject(final Object p_object) {
        m_object = p_object;
    }

    public void reset() {
        m_primitive = 0;
        m_object = null;
        m_startIndex = 0;
    }
}
