package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Context for a terminal command to access services and utility functions
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.04.2017
 */
public class TerminalCommandContext {
    private DXRAMServiceAccessor m_serviceAccessor;

    /**
     * Constructor
     *
     * @param p_serviceAccessor
     *     Service accessor for the terminal commands
     */
    public TerminalCommandContext(final DXRAMServiceAccessor p_serviceAccessor) {
        m_serviceAccessor = p_serviceAccessor;
    }

    /**
     * Print to the console
     *
     * @param p_str
     *     String to print
     */
    public static void print(final String p_str) {
        System.out.print(p_str);
    }

    /**
     * Print to the console + newline
     *
     * @param p_str
     *     String to print
     */
    public static void println(final String p_str) {
        System.out.println(p_str);
    }

    /**
     * Print to the console using a c-style formated string and arguments
     *
     * @param p_format
     *     Formatting for the string
     * @param p_args
     *     Optional arguments
     */
    public static void printf(final String p_format, final Object... p_args) {
        System.out.printf(p_format, p_args);
    }

    /**
     * Print an error message to the console
     *
     * @param p_str
     *     String to print
     */
    public static void printErr(final String p_str) {
        changeConsoleColor(TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
        System.out.print(p_str);
        changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
    }

    /**
     * Print an error message to the conssole + newline
     *
     * @param p_str
     *     String to print
     */
    public static void printlnErr(final String p_str) {
        changeConsoleColor(TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
        System.out.println(p_str);
        changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
    }

    /**
     * Print an error to the console using a c-style formated string and arguments
     *
     * @param p_format
     *     Formating for the string
     * @param p_args
     *     Optional arguments
     */
    public static void printfErr(final String p_format, final Object... p_args) {
        changeConsoleColor(TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
        System.out.printf(p_format, p_args);
        changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
    }

    /**
     * Print an error to the console using a c-style formated string and arguments + newline
     *
     * @param p_format
     *     Formating for the string
     * @param p_args
     *     Optional arguments
     */
    public static void printflnErr(final String p_format, final Object... p_args) {
        changeConsoleColor(TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
        System.out.printf(p_format, p_args);
        System.out.print('\n');
        changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
    }

    /**
     * Print to the console using a c-style formated string and arguments + newline
     *
     * @param p_format
     *     Formating for the string
     * @param p_args
     *     Optional arguments
     */
    public static void printfln(final String p_format, final Object... p_args) {
        System.out.printf(p_format, p_args);
        System.out.print('\n');
    }

    /**
     * Get a boolean argument from an argument list
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static boolean getArgBoolean(final String[] p_args, final int p_pos, final boolean p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return Boolean.parseBoolean(p_args[p_pos]);
    }

    /**
     * Get a short argument from an argument list
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static short getArgShort(final String[] p_args, final int p_pos, final short p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return Short.parseShort(p_args[p_pos]);
    }

    /**
     * Get an int argument from an argument list
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static int getArgInt(final String[] p_args, final int p_pos, final int p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return Integer.parseInt(p_args[p_pos]);
    }

    /**
     * Get a long argument from an argument list
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static long getArgLong(final String[] p_args, final int p_pos, final long p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return Long.parseLong(p_args[p_pos]);
    }

    /**
     * Get a string argument from an argument list
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static String getArgString(final String[] p_args, final int p_pos, final String p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return p_args[p_pos];
    }

    /**
     * Get a node role argument from an argument list. The argument must match one of the string representations of a NodeRole
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static NodeRole getArgNodeRole(final String[] p_args, final int p_pos, final NodeRole p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return NodeRole.toNodeRole(p_args[p_pos]);
    }

    /**
     * Get a node id argument from an argument list. The argument must be hex string, e.g. ABCD or 0xABCD
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static short getArgNodeId(final String[] p_args, final int p_pos, final short p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return NodeID.parse(p_args[p_pos]);
    }

    /**
     * Get a barrier id argument from an argument list. The argument must be hex string, e.g. ABCD or 0xABCD
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static int getArgBarrierId(final String[] p_args, final int p_pos, final int p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return BarrierID.parse(p_args[p_pos]);
    }

    /**
     * Get a local id argument from an argument list. The argument must be hex string, e.g. ABCD or 0xABCD
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static long getArgLocalId(final String[] p_args, final int p_pos, final long p_default) {
        return getArgChunkId(p_args, p_pos, p_default);
    }

    /**
     * Get a chunk id argument from an argument list. The argument must be hex string, e.g. ABCD or 0xABCD
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to get
     * @param p_default
     *     Default value if argument not available
     * @return Value
     */
    public static long getArgChunkId(final String[] p_args, final int p_pos, final long p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return ChunkID.parse(p_args[p_pos]);
    }

    /**
     * Check if an argument represents a chunk id (e.g. 0x1234567890ABCDEF or 1234567890ABCDEF)
     *
     * @param p_args
     *     Argument list
     * @param p_pos
     *     Position of the argument to check
     * @return True of argument matches a chunk id representation, false otherwise.
     */
    public static boolean isArgChunkID(final String[] p_args, final int p_pos) {
        String str = p_args[p_pos];

        if (str.startsWith("0x")) {
            str = str.substring(2);
        }

        return str.length() >= 16;
    }

    /**
     * Get a service from DXRAM.
     *
     * @param p_class
     *     Class of the service to get. If the service has different implementations, use the common interface
     *     or abstract class to get the registered instance.
     * @param <T>
     *     Class extending DXRAMService
     * @return Reference to the service if available and enabled, null otherwise.
     */
    public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        return m_serviceAccessor.getService(p_class);
    }

    /**
     * Change the color of stdout.
     *
     * @param p_color
     *     Text color.
     * @param p_backgroundColor
     *     Shell background color
     * @param p_style
     *     Text style.
     */
    private static void changeConsoleColor(final TerminalColor p_color, final TerminalColor p_backgroundColor, final TerminalStyle p_style) {
        if (p_backgroundColor != TerminalColor.DEFAULT) {
            System.out.printf("\033[%d;%d;%dm", p_style.ordinal(), p_color.ordinal() + 30, p_backgroundColor.ordinal() + 40);
        } else if (p_color != TerminalColor.DEFAULT) {
            System.out.printf("\033[%d;%dm", p_style.ordinal(), p_color.ordinal() + 30);
        } else {
            System.out.printf("\033[%dm", p_style.ordinal());
        }
    }
}
