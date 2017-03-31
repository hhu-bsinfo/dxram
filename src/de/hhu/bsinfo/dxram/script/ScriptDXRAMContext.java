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

package de.hhu.bsinfo.dxram.script;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * DXRAM script context (object) exposed to the java script engine
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 14.10.2016
 */
interface ScriptDXRAMContext {

    /**
     * List all available services (their short names)
     */
    void list();

    /**
     * Get a DXRAM service by its short name.
     *
     * @param p_serviceName
     *     DXRAM service short name
     * @return DXRAM service or null if the service does not exist
     */
    AbstractDXRAMService service(String p_serviceName);

    /**
     * Get the decimal part of a double value as int
     *
     * @param p_val
     *     Value
     * @return Decimal part as int
     */
    int toInt(final double p_val);

    /**
     * Get the decimal part of a double value as long
     *
     * @param p_val
     *     Value
     * @return Decimal part as long
     */
    long toLong(final double p_val);

    /**
     * Convert a short value to an unsigned hex string.
     *
     * @param p_val
     *     Short value to convert
     * @return Unsigned hex string (e.g. 0x1234)
     */
    String shortToHexStr(short p_val);

    /**
     * Convert an int value to an unsigned int hex string.
     *
     * @param p_val
     *     Value to convert
     * @return Unsigned hex string (e.g. 0x12345678)
     */
    String intToHexStr(int p_val);

    /**
     * Convert a long value to an unsigned long hex string
     *
     * @param p_val
     *     Value to convert
     * @return Unsigned hex string (e.g. 0x1234567812345678)
     */
    String longToHexStr(long p_val);

    /**
     * Convert a long string to a long value.
     *
     * @param p_str
     *     String to convert (e.g. 0x1234)
     * @return Long value of the long string
     * @note Java script does not support 64 bit values (max is 56 bit). Use strings instead and use this method to convert them to long objects
     */
    long longStrToLong(String p_str);

    /**
     * Convert a node role string to a node role object
     *
     * @param p_str
     *     String node role representation
     * @return NodeRole object
     */
    NodeRole nodeRole(String p_str);

    /**
     * Sleep for a specified time in ms.
     *
     * @param p_timeMs
     *     Number of ms to sleep
     */
    void sleep(int p_timeMs);

    /**
     * Create a cid of a separate nid and lid.
     *
     * @param p_nid
     *     Nid
     * @param p_lid
     *     Lid
     * @return Cid
     */
    long cid(short p_nid, long p_lid);

    /**
     * Get the nid part of the cid.
     *
     * @param p_cid
     *     Cid
     * @return Nid
     */
    short nidOfCid(long p_cid);

    /**
     * Get the lid part of the cid
     *
     * @param p_cid
     *     Cid
     * @return Lid
     */
    long lidOfCid(long p_cid);

    /**
     * Create a new DataStructure object. The DataStructure class needs to be
     * available in the dxram package.
     *
     * @param p_className
     *     Fully qualified class name with package path
     * @return New data structure instance
     */
    DataStructure newDataStructure(String p_className);

    /**
     * Read the contents of a (text) file.
     *
     * @param p_path
     *     Path of the file to read.
     * @return String containing the text of the file or null on error.
     */
    String readFile(String p_path);
}
