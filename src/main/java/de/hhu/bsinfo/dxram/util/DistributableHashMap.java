package de.hhu.bsinfo.dxram.util;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import de.hhu.bsinfo.dxutils.serialization.ClassUtil;
import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * A distributable HashMap using generic types.
 *
 * @param <K> The key's type.
 * @param <V> The value's type.
 *
 * @author Filip Krakowski, krakowski@uni-duesseldorf.de
 */
public class DistributableHashMap<K, V> extends HashMap<K, V> implements Distributable {

    private int m_size;
    private DistributableValue<K> m_keyHolder;
    private DistributableValue<V> m_valueHolder;
    private String m_keyHolderClass;
    private String m_valueHolderClass;

    public DistributableHashMap() {}

    public DistributableHashMap(Supplier<DistributableValue<K>> p_keySupplier,
            Supplier<DistributableValue<V>> p_valueSupplier) {
        m_keyHolder = p_keySupplier.get();
        m_valueHolder = p_valueSupplier.get();
        m_keyHolderClass = m_keyHolder.getClass().getName();
        m_valueHolderClass = m_valueHolder.getClass().getName();
    }

    public DistributableHashMap(int p_initialCapacity,
            Supplier<DistributableValue<K>> p_keySupplier,
            Supplier<DistributableValue<V>> p_valueSupplier) {
        super(p_initialCapacity);
        m_keyHolder = p_keySupplier.get();
        m_valueHolder = p_valueSupplier.get();
        m_keyHolderClass = m_keyHolder.getClass().getName();
        m_valueHolderClass = m_valueHolder.getClass().getName();
    }

    public DistributableHashMap(int p_initialCapacity, float p_loadFactor,
            Supplier<DistributableValue<K>> p_keySupplier,
            Supplier<DistributableValue<V>> p_valueSupplier) {
        super(p_initialCapacity, p_loadFactor);
        m_keyHolder = p_keySupplier.get();
        m_valueHolder = p_valueSupplier.get();
        m_keyHolderClass = m_keyHolder.getClass().getName();
        m_valueHolderClass = m_valueHolder.getClass().getName();
    }

    public DistributableHashMap(Map<? extends K, ? extends V> p_source,
            Supplier<DistributableValue<K>> p_keySupplier,
            Supplier<DistributableValue<V>> p_valueSupplier) {
        super(p_source);
        m_keyHolder = p_keySupplier.get();
        m_valueHolder = p_valueSupplier.get();
        m_keyHolderClass = m_keyHolder.getClass().getName();
        m_valueHolderClass = m_valueHolder.getClass().getName();
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_keyHolderClass);
        p_exporter.writeString(m_valueHolderClass);

        p_exporter.writeInt(size());

        for (Map.Entry<K, V> entry : entrySet()) {
            p_exporter.exportObject(wrapKey(entry.getKey()));
            p_exporter.exportObject(wrapValue(entry.getValue()));
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        m_keyHolderClass = p_importer.readString(m_keyHolderClass);
        m_valueHolderClass = p_importer.readString(m_valueHolderClass);

        if (m_keyHolder == null) {
            m_keyHolder = ClassUtil.createInstance(m_keyHolderClass);
        }

        if (m_valueHolder == null) {
            m_valueHolder = ClassUtil.createInstance(m_valueHolderClass);
        }

        m_size = p_importer.readInt(m_size);

        for (int i = 0; i < m_size; i ++) {
            p_importer.importObject(m_keyHolder);
            p_importer.importObject(m_valueHolder);
            put(m_keyHolder.getValue(), m_valueHolder.getValue());
        }
    }

    @Override
    public int sizeofObject() {
        int size = Integer.BYTES + ObjectSizeUtil.sizeofString(m_keyHolderClass) + ObjectSizeUtil.sizeofString(m_valueHolderClass);

        for (Map.Entry<K, V> entry : entrySet()) {
            size += wrapKey(entry.getKey()).sizeofObject();
            size += wrapValue(entry.getValue()).sizeofObject();
        }

        return size;
    }

    private DistributableValue<K> wrapKey(final K p_key) {
        m_keyHolder.setValue(p_key);
        return m_keyHolder;
    }

    private DistributableValue<V> wrapValue(final V p_value) {
        m_valueHolder.setValue(p_value);
        return m_valueHolder;
    }
}
