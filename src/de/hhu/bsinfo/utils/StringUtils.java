package de.hhu.bsinfo.utils;

import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

/**
 * Various string related utility functions.
 *
 * @author Florian Klein, florian.klein@hhu.de, 05.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public class StringUtils {

    /**
     * Utils class
     */
    private StringUtils() {

    }

    /**
     * Checks if a Byte array contains a String (as much precise as possible)
     *
     * @param p_array
     *     the byte array
     * @return whether the byte array contains a String
     * @throws UnsupportedEncodingException
     *     if the encoding is unsupported
     */
    public static boolean looksLikeUTF8(final byte[] p_array) throws UnsupportedEncodingException {
        String phonyString;
        Pattern p;

        p = Pattern.compile(
            "\\A(\n" + "  [\\x09\\x0A\\x0D\\x20-\\x7E]             # ASCII\\n" + "| [\\xC2-\\xDF][\\x80-\\xBF]               # non-overlong 2-byte\n" +
                "|  \\xE0[\\xA0-\\xBF][\\x80-\\xBF]         # excluding overlongs\n" + "| [\\xE1-\\xEC\\xEE\\xEF][\\x80-\\xBF]{2}  # straight 3-byte\n" +
                "|  \\xED[\\x80-\\x9F][\\x80-\\xBF]         # excluding surrogates\n" + "|  \\xF0[\\x90-\\xBF][\\x80-\\xBF]{2}      # planes 1-3\n" +
                "| [\\xF1-\\xF3][\\x80-\\xBF]{3}            # planes 4-15\n" + "|  \\xF4[\\x80-\\x8F][\\x80-\\xBF]{2}      # plane 16\n" + ")*\\z",
            Pattern.COMMENTS);

        phonyString = new String(p_array, "UTF-8");

        return p.matcher(phonyString).matches();
    }
}
