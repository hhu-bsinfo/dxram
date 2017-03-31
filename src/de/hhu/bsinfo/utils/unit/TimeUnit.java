/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.utils.unit;

/**
 * Wrapper for handling and converting time units (ns, us, ms, sec, min, h)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.10.2016
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
     * @param p_value
     *     Value
     * @param p_unit
     *     Unit of the value (ns, us, ms, sec, min, h)
     */
    public TimeUnit(final long p_value, final String p_unit) {
        parse(p_value, p_unit);
    }

    /**
     * Get as ns
     *
     * @return Ns
     */
    public long getNs() {
        return m_timeNs;
    }

    /**
     * Get as us
     *
     * @return Us
     */
    public long getUs() {
        return m_timeNs / 1000;
    }

    /**
     * Get as ms
     *
     * @return Ms
     */
    public long getMs() {
        return m_timeNs / 1000 / 1000;
    }

    /**
     * Get as sec
     *
     * @return Sec
     */
    public long getSec() {
        return m_timeNs / 1000 / 1000 / 1000;
    }

    /**
     * Get as min
     *
     * @return Min
     */
    public long getMin() {
        return m_timeNs / 1000 / 1000 / 1000 / 60;
    }

    /**
     * Get as hours
     *
     * @return Hours
     */
    public long getHours() {
        return m_timeNs / 1000 / 1000 / 1000 / 60 / 60;
    }

    /**
     * Get the time in seconds
     *
     * @return Time in seconds as double value
     */
    public double getSecDouble() {
        return ((double) m_timeNs) / 1000.0 / 1000.0 / 1000.0;
    }

    /**
     * Parse the value with the specified unit
     *
     * @param p_value
     *     Value
     * @param p_unit
     *     Unit of the value
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
