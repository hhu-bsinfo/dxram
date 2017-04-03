package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Created by nothaas on 4/3/17.
 */
// TODO doc
public class TerminalCommandContext {

    private DXRAMServiceAccessor m_serviceAccessor;

    public TerminalCommandContext(final DXRAMServiceAccessor p_serviceAccessor) {
        m_serviceAccessor = p_serviceAccessor;
    }

    public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        return m_serviceAccessor.getService(p_class);
    }

    /**
     * Print to the console
     *
     * @param p_str
     *     String to print
     */
    public void print(final String p_str) {
        System.out.print(p_str);
    }

    /**
     * Print to the console + newline
     *
     * @param p_str
     *     String to print
     */
    public void println(final String p_str) {
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
    public void printf(final String p_format, final Object... p_args) {
        System.out.printf(p_format, p_args);
    }

    /**
     * Print an error message to the console
     *
     * @param p_str
     *     String to print
     */
    public void printErr(final String p_str) {
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
    public void printlnErr(final String p_str) {
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
    public void printfErr(final String p_format, final Object... p_args) {
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
    public void printflnErr(final String p_format, final Object... p_args) {
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
    public void printfln(final String p_format, final Object... p_args) {
        System.out.printf(p_format, p_args);
        System.out.print('\n');
    }

    public boolean getArgBoolean(final String[] p_args, final int p_pos, final boolean p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return Boolean.parseBoolean(p_args[p_pos]);
    }

    public short getArgShort(final String[] p_args, final int p_pos, final short p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return Short.parseShort(p_args[p_pos]);
    }

    public int getArgInt(final String[] p_args, final int p_pos, final int p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return Integer.parseInt(p_args[p_pos]);
    }

    public long getArgLong(final String[] p_args, final int p_pos, final long p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return Long.parseLong(p_args[p_pos]);
    }

    public String getArgString(final String[] p_args, final int p_pos, final String p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return p_args[p_pos];
    }

    public NodeRole getArgNodeRole(final String[] p_args, final int p_pos, final NodeRole p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return NodeRole.toNodeRole(p_args[p_pos]);
    }

    public short getArgNodeId(final String[] p_args, final int p_pos, final short p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return NodeID.parse(p_args[p_pos]);
    }

    public long getArgLocalId(final String[] p_args, final int p_pos, final long p_default) {
        return getArgChunkId(p_args, p_pos, p_default);
    }

    public long getArgChunkId(final String[] p_args, final int p_pos, final long p_default) {
        if (p_args.length <= p_pos) {
            return p_default;
        }

        return ChunkID.parse(p_args[p_pos]);
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
    private void changeConsoleColor(final TerminalColor p_color, final TerminalColor p_backgroundColor, final TerminalStyle p_style) {
        if (p_backgroundColor != TerminalColor.DEFAULT) {
            System.out.printf("\033[%d;%d;%dm", p_style.ordinal(), p_color.ordinal() + 30, p_backgroundColor.ordinal() + 40);
        } else if (p_color != TerminalColor.DEFAULT) {
            System.out.printf("\033[%d;%dm", p_style.ordinal(), p_color.ordinal() + 30);
        } else {
            System.out.printf("\033[%dm", p_style.ordinal());
        }
    }
}
