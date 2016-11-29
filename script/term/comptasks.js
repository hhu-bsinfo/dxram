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

	return "List all available tasks that are registered and can be executed"
}

function exec(nid) {

    var mscomp = dxram.service("mscomp");
    var payloads = mscomp.getRegisteredTaskPayloads();

    dxterm.printfln("Registered task payload classes (%d): ", payloads.size());
    for each (payload in payloads) {
        dxterm.printfln("%s: 0x%X, 0x%X",
            payload.getValue().getSimpleName(), (payload.getKey() >> 16 & 0xFFFF), (payload.getKey() & 0xFFFF));
    }
}
