/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxmonitor.error;

/**
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class CantReadFileException extends Exception {
    /**
     * Throws an Exception because the given path is invalid.
     *
     * @param p_file
     *     Path of the file
     */
    public CantReadFileException(final String p_file) {
        super("Error: Couldn't read from file " + p_file + '!');
    }
}
