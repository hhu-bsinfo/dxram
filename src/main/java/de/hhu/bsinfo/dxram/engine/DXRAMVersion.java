package de.hhu.bsinfo.dxram.engine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /**
     * Check if this version is compatible to another one
     *
     * @param p_other
     *         Other version to compare to
     * @return True if compatible, false otherwise.
     */
    public boolean compareCompatibility(final DXRAMVersion p_other) {
        return m_major == p_other.m_major && m_minor == p_other.m_minor;
    }

    /**
     * Create a DXRAMVersion object from a string
     *
     * @param p_version
     *         Version string
     * @return DXRAMVersion object
     */
    public static DXRAMVersion fromString(final String p_version) {
        Pattern pattern = Pattern.compile("^(\\d+).(\\d+).(\\d+)(?:-\\w+)*$");

        Matcher matcher = pattern.matcher(p_version);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid version string");
        }

        int major = Integer.valueOf(matcher.group(1));
        int minor = Integer.valueOf(matcher.group(2));
        int revision = Integer.valueOf(matcher.group(3));

        return new DXRAMVersion(major, minor, revision);
    }

    @Override
    public String toString() {
        return String.format("%d.%d.%d", m_major, m_minor, m_revision);
    }
}
