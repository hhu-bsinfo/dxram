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

package de.hhu.bsinfo.dxgraph.util.args;

/**
 * Interface for a parser to process the string array provided by the java main function
 * to create a MainArguments list of.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public interface ArgumentListParser {
    /**
     * Parse the string array provided by the java main function.
     *
     * @param p_args
     *         Array of strings from the java main function.
     * @param p_arguments
     *         Argument list to add the parsed arguments to.
     */
    void parseArguments(String[] p_args, ArgumentList p_arguments);
}
