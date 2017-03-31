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

function imports() {

}

function help() {

	return "Change the output level of the logger\n" +
			"Usage (1): loggerlevel(level)\n" +
			"Usage (2): loggerlevel(level, nid)\n" +
			"  level: Log level to set, available levels (str): disabled, error, warn, info, debug, trace\n" +
			"  nid: Change the log level of another node, defaults to current node";
}

function exec(level, nid) {

    if (level == null) {
        dxterm.printlnErr("No level specified");
        return;
    }

    var logger = dxram.service("logger");

    if (nid == null) {
        logger.setLogLevel(level);
    } else {
        logger.setLogLevel(level, nid);
    }
}
