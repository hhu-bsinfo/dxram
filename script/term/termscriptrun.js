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

function imports() {

}

function help() {
	return "Load and run a java script file within the DXRAM terminal context\n" +
			"Parameters: scriptfile\n" +
			"  scriptfile: Path to script file to load and run";
}

function exec(scriptfile) {

    if (scriptfile == null) {
        dxterm.printlnErr("No scriptfile specified");
        return;
    }

	var term = dxram.service("term");

    if (!term.load(scriptfile)) {
        dxterm.printf("Loading and running script file %s failed\n", scriptfile);
    }
}
