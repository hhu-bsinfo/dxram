package de.hhu.bsinfo.dxram.migration.data;

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class MigrationIdentifier implements Exportable, Importable {

    private static final AtomicLong ID_COUNTER = new AtomicLong(0);

    private short m_source;
    private short m_target;
    private long m_id;

    public MigrationIdentifier() {

    }

    public MigrationIdentifier(short p_source, short p_target) {
        m_source = p_source;
        m_target = p_target;
        m_id = ID_COUNTER.getAndIncrement();
    }

    public short getSource() {
        return m_source;
    }

    public short getTarget() {
        return m_target;
    }

    public long getId() {
        return m_id;
    }

    @Override
    public boolean equals(Object p_o) {
        if (this == p_o) return true;
        if (p_o == null || getClass() != p_o.getClass()) return false;
        MigrationIdentifier that = (MigrationIdentifier) p_o;
        return m_source == that.m_source &&
                m_target == that.m_target &&
                m_id == that.m_id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_source, m_target, m_id);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeShort(m_source);
        p_exporter.writeShort(m_target);
        p_exporter.writeLong(m_id);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_source = p_importer.readShort(m_source);
        m_target = p_importer.readShort(m_target);
        m_id = p_importer.readLong(m_id);
    }

    @Override
    public int sizeofObject() {
        return 2 * Short.BYTES + Long.BYTES;
    }

    @Override
    public String toString() {
        return String.format("[%04X.%04X](%d)", m_source, m_target, m_id);
    }
}
