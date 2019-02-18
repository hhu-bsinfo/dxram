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

import de.hhu.bsinfo.dxram.ms.ComputeRole;
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
     * Timeout for the test in milliseconds. If the test did not complete within the given deadline, abort
     * and report as failed.
     *
     * @return Timeout in ms. -1 for no timeout.
     */
    int timeoutMs() default -1;

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
         * The total size of the backend key-value storage.
         *
         * @return Total size of key-value storage in MB.
         */
        long keyValueStorageSizeMB() default 128;

        /**
         * Enable/disable the key-value backend storage.
         *
         * @return True to enable, false to disable.
         */
        boolean enableKeyValueStorage() default true;

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

        /**
         * Enable the job subsystem
         *
         * @return True to enable, false disable
         */
        boolean enableJobService() default false;

        /**
         * Enable the master slave service and set the role for the compute node
         *
         * @return Role of the node
         */
        ComputeRole masterSlaveComputeRole() default ComputeRole.NONE;

        /**
         * Set the request response timeout for network messages.
         * Have a rather high timeout for localhost testing because, naturally, everything will be slower.
         *
         * @return Timeout in ms.
         */
        int networkRequestResponseTimeoutMs() default 1000;
    }
}
