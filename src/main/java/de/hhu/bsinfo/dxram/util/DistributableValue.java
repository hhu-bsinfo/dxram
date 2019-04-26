package de.hhu.bsinfo.dxram.util;

import de.hhu.bsinfo.dxutils.serialization.Distributable;

public abstract class DistributableValue<T> implements Distributable {

    protected T m_value;

    public T getValue() {
        return m_value;
    }

    void setValue(final T p_value) {
        m_value = p_value;
    }
}
