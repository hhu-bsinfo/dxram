package de.hhu.bsinfo.dxram.loader;

import lombok.Getter;

import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class LoaderJar implements Distributable {
    @Getter
    private byte[] m_jarBytes;
    @Getter
    private int m_version;

    public LoaderJar() {

    }

    public LoaderJar(byte[] p_jarBytes, int p_version) {
        m_jarBytes = p_jarBytes;
        m_version = p_version;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeByteArray(m_jarBytes);
        p_exporter.writeInt(m_version);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_jarBytes = p_importer.readByteArray(m_jarBytes);
        m_version = p_importer.readInt(m_version);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofByteArray(m_jarBytes) + Integer.BYTES;
    }
}
