package de.hhu.bsinfo.dxram.migration.data;

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.Objects;

public class MigrationIdentifier implements Exportable, Importable {

    private short m_source;

    private short m_target;

    private long m_startId;

    private long m_endId;

    public MigrationIdentifier() {

    }

    public MigrationIdentifier(short p_source, short p_target, long p_startId, long p_endId) {

        m_source = p_source;

        m_target = p_target;

        m_startId = p_startId;

        m_endId = p_endId;
    }

    public short getSource() {

        return m_source;
    }

    public short getTarget() {

        return m_target;
    }

    public long getStartId() {

        return m_startId;
    }

    public long getEndId() {

        return m_endId;
    }

    @Override
    public boolean equals(Object p_o) {

        if (this == p_o) {

            return true;
        }

        if (p_o == null || getClass() != p_o.getClass()) {

            return false;
        }

        MigrationIdentifier that = (MigrationIdentifier) p_o;

        return m_source == that.m_source && m_target == that.m_target &&
                m_startId == that.m_startId && m_endId == that.m_endId;
    }

    @Override
    public int hashCode() {

        return Objects.hash(m_source, m_target, m_startId, m_endId);
    }

    @Override
    public void exportObject(Exporter p_exporter) {

        p_exporter.writeShort(m_source);

        p_exporter.writeShort(m_target);

        p_exporter.writeLong(m_startId);

        p_exporter.writeLong(m_endId);
    }

    @Override
    public void importObject(Importer p_importer) {

        m_source = p_importer.readShort(m_source);

        m_target = p_importer.readShort(m_target);

        m_startId = p_importer.readLong(m_startId);

        m_endId = p_importer.readLong(m_endId);
    }

    @Override
    public int sizeofObject() {

        return 2 * Short.BYTES + 2 * Long.BYTES;
    }
}
