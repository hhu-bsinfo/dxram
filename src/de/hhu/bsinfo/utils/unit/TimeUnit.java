package de.hhu.bsinfo.utils.unit;

/**
 * Wrapper for handling and converting time units (ns, us, ms, sec, min, h)
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.10.16
 */
public class TimeUnit {

    public static final String NS = "ns";
    public static final String US = "us";
    public static final String MS = "ms";
    public static final String SEC = "sec";
    public static final String MIN = "min";
    public static final String H = "h";

    private long m_timeNs;

    /**
     * Constructor
     *
     * @param p_value Value
     * @param p_unit Unit of the value (ns, us, ms, sec, min, h)
     */
    public TimeUnit(final long p_value, final String p_unit) {
        parse(p_value, p_unit);
    }

    /**
     * Get as ns
     * @return Ns
     */
    public long getNs() {
        return m_timeNs;
    }

    /**
     * Get as us
     * @return Us
     */
    public long getUs() {
        return m_timeNs / 1000;
    }

    /**
     * Get as ms
     * @return Ms
     */
    public long getMs() {
        return m_timeNs / 1000 / 1000;
    }

    /**
     * Get as sec
     * @return Sec
     */
    public long getSec() {
        return m_timeNs / 1000 / 1000 / 1000;
    }

    /**
     * Get as min
     * @return Min
     */
    public long getMin() {
        return m_timeNs / 1000 / 1000 / 1000 / 60;
    }

    /**
     * Get as hours
     * @return Hours
     */
    public long getHours() {
        return m_timeNs / 1000 / 1000 / 1000 / 60 / 60;
    }

    /**
     * Parse the value with the specified unit
     *
     * @param p_value Value
     * @param p_unit Unit of the value
     */
    private void parse(final long p_value, final String p_unit) {
        switch (p_unit) {
            case US:
                m_timeNs = p_value * 1000;
                break;
            case MS:
                m_timeNs = p_value * 1000 * 1000;
                break;
            case SEC:
                m_timeNs = p_value * 1000 * 1000 * 1000;
                break;
            case MIN:
                m_timeNs = p_value * 1000 * 1000 * 1000 * 60;
                break;
            case H:
                m_timeNs = p_value * 1000 * 1000 * 1000 * 60 * 60;
                break;
            case NS:
            default:
                m_timeNs = p_value;
        }
    }
}
