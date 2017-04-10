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

package de.hhu.bsinfo.utils.serialization;

/**
 * Interface defining an object which can be imported/de-serialized.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.12.2015
 */
public interface Importable extends ObjectSize {
    /**
     * TaskScriptImport/De-serialize this object using the provided importer (target).
     * Don't call this explicitly. Use an Importer which is calling this
     * method implicitly.
     *
     * @param p_importer
     *     Target to import/de-serialize the object from.
     */
    void importObject(final Importer p_importer);
}
