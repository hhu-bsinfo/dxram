/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.utils;

/**
 * Wrapper for accessing GNU readline lib
 *
 * @author Michael Schoettner, michael.schoettner@hhu.de, 07.09.2015
 */
public final class JNIconsole {

    /**
     * Constructor
     */
    private JNIconsole() {
    }

    /**
     * Provide the path to the native implementation.
     *
     * @param p_pathNativeLibrary
     *     Path to the library with the native implementation.
     */
    public static void load(final String p_pathNativeLibrary) {
        System.load(p_pathNativeLibrary);
    }

    /**
     * Provide commands for autocomplete using the tab key and enable it.
     *
     * @param p_commands
     *     Commands to be used for autocomplete
     */
    public static native void autocompleteCommands(String[] p_commands);

    /**
     * Add a string to the history of the console.
     * Use this to load a history from a file and add the commands executed.
     *
     * @param p_str
     *     String to add to the history.
     */
    public static native void addToHistory(String p_str);

    /**
     * Read one line from console.
     *
     * @param p_prompt
     *     The terminal prompt to display.
     * @return the read line
     */
    public static native byte[] readline(String p_prompt);

}
