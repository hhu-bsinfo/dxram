package de.hhu.bsinfo.utils;

/**
 * Wrapper for handling and converting storage units (byte, kb, mb, gb, tb)
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.10.16
 */
public class StorageUnit {

    public static final String UNIT_BYTE = "b";
    public static final String UNIT_KB = "kb";
    public static final String UNIT_MB = "mb";
    public static final String UNIT_GB = "gb";
    public static final String UNIT_TB = "tb";

    private long m_bytes;

    /**
     * Constructor
     *
     * @param p_value Value
     * @param p_unit Unit of the value (b, kb, mb, gb, tb)
     */
    public StorageUnit(final long p_value, final String p_unit) {
        parse(p_value, p_unit.toLowerCase());
    }

    /**
     * Get as bytes
     * @return Bytes
     */
    public long getBytes() {
        return m_bytes;
    }

    /**
     * Get as KB
     * @return KB
     */
    public long getKB() {
        return m_bytes / 1024;
    }

    /**
     * Get as MB
     * @return MB
     */
    public long getMB() {
        return m_bytes / 1024 / 1024;
    }

    /**
     * Get as GB
     * @return GB
     */
    public long getGB() {
        return m_bytes / 1024 / 1024 / 1024;
    }

    /**
     * Get as TB
     * @return TB
     */
    public long getTB() {
        return m_bytes / 1024 / 1024 / 1024 / 1024;
    }

    @Override
    public String toString() {
        return m_bytes + " bytes";
    }

    /**
     * Prase the value with the specified unit
     *
     * @param p_value Value
     * @param p_unit Unit of the value
     */
    private void parse(final long p_value, final String p_unit) {
        switch (p_unit) {
            case UNIT_KB:
                m_bytes = p_value * 1024;
                break;
            case UNIT_MB:
                m_bytes = p_value * 1024 * 1024;
                break;
            case UNIT_GB:
                m_bytes = p_value * 1024 * 1024 * 1024;
                break;
            case UNIT_TB:
                m_bytes = p_value * 1024 * 1024 * 1024 * 1024;
                break;
            case UNIT_BYTE:
            default:
                m_bytes = p_value;
                break;
        }
    }
}
