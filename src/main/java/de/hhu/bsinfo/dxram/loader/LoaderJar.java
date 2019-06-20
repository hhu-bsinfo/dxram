package de.hhu.bsinfo.dxram.loader;

import lombok.Getter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class LoaderJar implements Distributable {
    @Getter
    private byte[] m_jarBytes;
    @Getter
    private int m_version;
    @Getter
    private String m_name;

    private static final Logger LOGGER = LogManager.getFormatterLogger(LoaderJar.class);

    public LoaderJar() {

    }

    public LoaderJar(String p_name) {
        m_name = p_name;
        m_version = -1;
        m_jarBytes = new byte[0];
    }

    public LoaderJar(byte[] p_jarBytes, int p_version, String p_name) {
        m_jarBytes = p_jarBytes;
        m_version = p_version;
        m_name = p_name;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeByteArray(m_jarBytes);
        p_exporter.writeInt(m_version);
        p_exporter.writeString(m_name);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_jarBytes = p_importer.readByteArray(m_jarBytes);
        m_version = p_importer.readInt(m_version);
        m_name = p_importer.readString(m_name);
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        size += ObjectSizeUtil.sizeofByteArray(m_jarBytes);
        size += Integer.BYTES;
        size += ObjectSizeUtil.sizeofString(m_name);

        return size;
    }

    public void writeToPath(Path p_path) {
        try {
            LOGGER.info(String.format("write file %s", p_path.toString()));
            Files.write(p_path, m_jarBytes);
        } catch (IOException e) {
            LOGGER.error(e);
        }
    }
}
