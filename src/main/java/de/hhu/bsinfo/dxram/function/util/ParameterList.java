package de.hhu.bsinfo.dxram.function.util;

import java.util.Arrays;
import java.util.stream.Stream;

import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class ParameterList implements Distributable {

    private String[] m_parameters;
    private int m_size;

    public ParameterList() {}

    public ParameterList(final String[] p_parameters) {
        m_parameters = p_parameters;
        m_size = p_parameters.length;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeCompactNumber(m_parameters.length);
        for (int i = 0; i < m_parameters.length; i++) {
            p_exporter.writeString(m_parameters[i]);
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        m_size = p_importer.readCompactNumber(m_size);

        if (m_parameters == null) {
            m_parameters = new String[m_size];
        }

        for (int i = 0; i < m_size; i++) {
            m_parameters[i] = p_importer.readString(m_parameters[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = ObjectSizeUtil.sizeofCompactedNumber(m_size);

        for (int i = 0; i < m_parameters.length; i++) {
            size += ObjectSizeUtil.sizeofString(m_parameters[i]);
        }

        return size;
    }

    public String[] getParameters() {
        return m_parameters;
    }

    public Stream<String> stream() {
        return Arrays.stream(m_parameters);
    }

    public String get(final int p_index) {
        if (p_index >= m_parameters.length) {
            return null;
        }

        return m_parameters[p_index];
    }
}
