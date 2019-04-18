/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxutils.serialization.ObjectSize;

import static de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil.*;

public enum ProgrammingLanguage implements ObjectSize {

    B,
    Basic,
    Bash,
    C,
    CPP,
    C_Sharp,
    D,
    F_Sharp,
    Fortran,
    Go,
    Haskell,
    Java,
    JavaScript,
    Kotlin,
    Objective_C,
    Pascal,
    Python,
    Ruby,
    Rust,
    Scala,
    Swift,
    TurboPascal,
    TypeScript,
    Visual_Basic,
    XSLT,
    Xtend;

    @Override
    public int sizeofObject() {
        return Integer.BYTES;
    }
}