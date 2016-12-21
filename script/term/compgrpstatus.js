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

	return "Get the current status of a compute group\n" +
	        "Usage: compgrpstatus(cgid)\n" +
            "  cgid: Compute group id to get the status from";
}

function exec(cgid) {

    if (cgid == null) {
        dxterm.printlnErr("No cgid specified");
        return;
    }

    var mscomp = dxram.service("mscomp");
    var status = mscomp.getStatusMaster(cgid);

    if (status == null) {
        dxterm.printflnErr("Getting compute group status of group %d failed", cgid);
        return;
    }

    dxterm.printfln("Status of group %d:\n%s", cgid, status);
}
