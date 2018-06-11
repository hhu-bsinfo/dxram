/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxterm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxterm.converter.ArgumentConverter;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Process a string and create a terminal command which splits the input text into tokens of command name and arguments for the command
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
public class TerminalCommandString implements Serializable {
    private String m_name = "";
    private String[] m_args = new String[0];
    private boolean m_isComment = false;

    private static final String SEPERATOR = "=";

    private static final String NO_ARG = "";

    /**
     * Constructor
     *
     * @param p_str
     *         Input string to process
     */
    protected TerminalCommandString(final String p_str) {
        // trim front and end
        String str = p_str.trim();

        if (str.startsWith("#")) {
            m_isComment = true;
            m_name = p_str;
            return;
        }

        // separate by space but keep strings, e.g. "this is a test" as a single string and remove the quotes
        List<String> list = new ArrayList<String>();
        Matcher matcher = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(str);
        while (matcher.find()) {
            list.add(matcher.group(1).replace("\"", ""));
        }

        // remove empty tokens due to multiple spaces
        list.removeIf(String::isEmpty);

        String[] tokens = list.toArray(new String[list.size()]);

        if (tokens.length > 0) {
            m_name = tokens[0];

            if (tokens.length > 1) {
                m_args = Arrays.copyOfRange(tokens, 1, tokens.length);
            }
        }
    }

    /**
     * Get the command's name
     *
     * @return Name
     */
    public String getName() {
        return m_name;
    }

    /**
     * Get the argument count
     *
     * @return Argument count
     */
    public int getArgc() {
        return m_args.length;
    }

    /**
     * Get the command's arguments
     *
     * @return Command arguments
     */
    public String[] getArgs() {
        return m_args;
    }

    /**
     * Check if the command is a comment. The full comment is stored as name.
     *
     * @return True if not an actual command but a comment
     */
    public boolean isComment() {
        return m_isComment;
    }

    /**
     * Get a boolean argument from an argument list
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public boolean getArgBoolean(final int p_pos, final boolean p_default) {
        if (m_args.length <= p_pos) {
            return p_default;
        }

        return Boolean.parseBoolean(m_args[p_pos]);
    }

    /**
     * Searches the specified argument within the argument list.
     *
     * @param p_name The argument's name.
     * @return The argument's value if it's present; otherwise an empty String.
     */
    public String getNamedArgument(final String p_name) {

        final String prefix = p_name + SEPERATOR;

        return Arrays.stream(m_args)
                .filter(arg -> arg.startsWith(prefix))
                .map(arg -> arg.split(SEPERATOR)[1])
                .findFirst()
                .orElse(NO_ARG);
    }

    /**
     * Searches the specified argument within the argument list and converts it using the provided converter.
     *
     * @param p_name The argument's name.
     * @return The argument's value if it's present; otherwise the specified default value.
     */
    public <T> T getNamedArgument(final String p_name, final ArgumentConverter<T> p_converter, T p_default) {

        String arg = getNamedArgument(p_name);

        return arg.equals(NO_ARG) ? p_default : p_converter.convert(arg);
    }

    /**
     * Get a short argument from an argument list
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public short getArgShort(final int p_pos, final short p_default) {
        if (m_args.length <= p_pos) {
            return p_default;
        }

        return Short.parseShort(m_args[p_pos]);
    }

    /**
     * Get an int argument from an argument list
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public int getArgInt(final int p_pos, final int p_default) {
        if (m_args.length <= p_pos) {
            return p_default;
        }

        return Integer.parseInt(m_args[p_pos]);
    }

    /**
     * Get a long argument from an argument list
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public long getArgLong(final int p_pos, final long p_default) {
        if (m_args.length <= p_pos) {
            return p_default;
        }

        return Long.parseLong(m_args[p_pos]);
    }

    /**
     * Get a string argument from an argument list
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public String getArgString(final int p_pos, final String p_default) {
        if (m_args.length <= p_pos) {
            return p_default;
        }

        return m_args[p_pos];
    }

    /**
     * Get a node role argument from an argument list. The argument must match one of the string representations of a NodeRole
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public NodeRole getArgNodeRole(final int p_pos, final NodeRole p_default) {
        if (m_args.length <= p_pos) {
            return p_default;
        }

        return NodeRole.toNodeRole(m_args[p_pos]);
    }

    /**
     * Get a node id argument from an argument list. The argument must be hex string, e.g. ABCD or 0xABCD
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public short getArgNodeId(final int p_pos, final short p_default) {
        if (m_args.length <= p_pos) {
            return p_default;
        }

        return NodeID.parse(m_args[p_pos]);
    }

    /**
     * Get a barrier id argument from an argument list. The argument must be hex string, e.g. ABCD or 0xABCD
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public int getArgBarrierId(final int p_pos, final int p_default) {
        if (m_args.length <= p_pos) {
            return p_default;
        }

        return BarrierID.parse(m_args[p_pos]);
    }

    /**
     * Get a local id argument from an argument list. The argument must be hex string, e.g. ABCD or 0xABCD
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public long getArgLocalId(final int p_pos, final long p_default) {
        return getArgChunkId(p_pos, p_default);
    }

    /**
     * Get a chunk id argument from an argument list. The argument must be hex string, e.g. ABCD or 0xABCD
     *
     * @param p_pos
     *         Position of the argument to get
     * @param p_default
     *         Default value if argument not available
     * @return Value
     */
    public long getArgChunkId(final int p_pos, final long p_default) {
        if (m_args.length <= p_pos) {
            return p_default;
        }

        return ChunkID.parse(m_args[p_pos]);
    }

    /**
     * Check if an argument represents a chunk id (e.g. 0x1234567890ABCDEF or 1234567890ABCDEF)
     *
     * @param p_pos
     *         Position of the argument to check
     * @return True of argument matches a chunk id representation, false otherwise.
     */
    public boolean isArgChunkID(final int p_pos) {
        if (m_args.length <= p_pos) {
            return false;
        }

        String str = m_args[p_pos];

        if (str.startsWith("0x")) {
            str = str.substring(2);
        }

        return str.length() >= 16;
    }

    @Override
    public String toString() {
        String str = "";

        str += m_name;

        for (int i = 0; i < m_args.length; i++) {
            str += " ";
            str += '"' + m_args[i] + '"';
        }

        return str;
    }
}
