package de.hhu.bsinfo.dxram.engine;

import java.util.Arrays;

/**
 * DXRAM version object
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2017
 */
public class DXRAMVersion {
    private final int m_major;
    private final int m_minor;
    private final int m_revision;

    public DXRAMVersion(final int p_major, final int p_minor, final int p_revision) {
        m_major = p_major;
        m_minor = p_minor;
        m_revision = p_revision;
    }

    /**
     * Get the major version number
     *
     * @return Major version number
     */
    public int getMajor() {
        return m_major;
    }

    /**
     * Get the minor version number
     *
     * @return Minor version number
     */
    public int getMinor() {
        return m_minor;
    }

    /**
     * Get the revision number
     *
     * @return Revision number
     */
    public int getRevision() {
        return m_revision;
    }

    public static final DXRAMVersion fromString(String p_version) {

        if (p_version.contains("-")) {

            int index = p_version.indexOf('-');

            p_version = p_version.substring(0, index);
        }

        int[] components = Arrays.stream(p_version.split("."))
                .mapToInt(Integer::valueOf)
                .limit(3)
                .toArray();

        return new DXRAMVersion(components[0], components[1], components[2]);
    }

    @Override
    public String toString() {
        return "DXRAMVersion " + m_major + '.' + m_minor + '.' + m_revision;
    }
}
