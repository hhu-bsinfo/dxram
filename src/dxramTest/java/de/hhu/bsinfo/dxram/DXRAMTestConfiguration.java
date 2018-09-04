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

package de.hhu.bsinfo.dxram;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Configuration for DXRAM uinit test class
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DXRAMTestConfiguration {
    /**
     * Node index (in order they are declared in nodes list) of node to run the test on
     *
     * @return Node index
     */
    int runTestOnNodeIdx();

    /**
     * List of nodes to spawn for the test (spawned in order!)
     *
     * @return List of nodes to spawn
     */
    Node[] nodes();

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface Node {
        /**
         * Role of the node to spawn
         *
         * @return Node role
         */
        NodeRole nodeRole();

        /**
         * Activate/Disable the backup. This parameter should be either active for all nodes or inactive for all nodes
         */
        boolean backupActive() default false;

        /**
         * This parameter can be set to false for single peers to avoid storing backups and the associated overhead.
         * If this peer is not available for backup, it will not log and recover chunks but all other backup functions,
         * like replicating own chunks, are enabled.
         * Do not set this parameter globally to deactivate backup. Use backupActive parameter for that purpose.
         */
        boolean availableForBackup() default false;
    }
}
