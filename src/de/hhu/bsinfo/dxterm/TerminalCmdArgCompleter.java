package de.hhu.bsinfo.dxterm;

import jline.console.ConsoleReader;
import jline.console.CursorBuffer;
import jline.console.completer.Completer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Custom completer to enable completions of command arguments
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.05.2017
 */
public class TerminalCmdArgCompleter implements Completer {
    private final ConsoleReader m_reader;
    private final TerminalSession m_session;

    /**
     * Constructor
     *
     * @param p_reader
     *         The console reader used for the terminal
     * @param p_session
     *         The current terminal session with the server
     */
    public TerminalCmdArgCompleter(final ConsoleReader p_reader, final TerminalSession p_session) {
        m_reader = p_reader;
        m_session = p_session;
    }

    @Override
    public int complete(final String p_buffer, final int p_cursor, final List<CharSequence> p_candidates) {
        CursorBuffer buffer = m_reader.getCursorBuffer();
        List<String> cmdTokens = new ArrayList<String>();
        int argPos = getCmd(buffer, cmdTokens);

        if (!cmdTokens.isEmpty()) {
            TerminalCommandString cmdStr = new TerminalCommandString(buffer.toString());

            if (!m_session.write(new TerminalReqCmdArgComp(argPos, cmdStr))) {
                return -1;
            }

            Object obj = m_session.read();

            if (obj instanceof TerminalCmdArgCompList) {
                List<String> tmp = ((TerminalCmdArgCompList) obj).getCompletions();
                if (tmp != null) {
                    SortedSet<String> list = new TreeSet<String>();
                    list.addAll(tmp);

                    if (p_buffer == null) {
                        p_candidates.addAll(list);
                    } else {
                        Iterator var4 = list.tailSet(p_buffer).iterator();

                        while (var4.hasNext()) {
                            String match = (String) var4.next();
                            if (!match.startsWith(p_buffer)) {
                                break;
                            }

                            p_candidates.add(match);
                        }
                    }
                }
            }
        }

        return p_candidates.isEmpty() ? -1 : 0;
    }

    /**
     * Helper to determine the current command entered and the argument position of the cursor
     *
     * @param p_buffer
     *         CursorBuffer of jline
     * @param p_tokens
     *         List to add the tokens of the currently entered command to
     * @return Current argument position depending on the CursorBuffer
     */
    private static int getCmd(final CursorBuffer p_buffer, final List<String> p_tokens) {
        int argPos;
        String str = p_buffer.toString();

        // separate by space but keep strings, e.g. "this is a test" as a single string and remove the quotes
        Matcher matcher = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(str);
        while (matcher.find()) {
            p_tokens.add(matcher.group(1).replace("\"", ""));
        }

        argPos = p_tokens.size() - 2;

        if (str.charAt(str.length() - 1) == ' ' || str.charAt(str.length() - 1) == '\t') {
            argPos++;
        }

        return argPos;
    }
}
