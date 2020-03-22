/*
 * Copyright (c) 2018.
 *
 * Author: Netium (Bo Zhou)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.netium.server.cluster.raft;

import org.netium.server.storageengine.StorageEngine;
import org.netium.network.telnet.TelnetCommandExecutor;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.protocols.raft.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ZeusStateMachine implements StateMachine, RAFT.RoleChange {
    private final Logger logger = LoggerFactory.getLogger(ZeusStateMachine.class);

    private final StorageEngine storageEngine;
    private final TelnetCommandExecutor executor;

    public ZeusStateMachine(final StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
        this.executor = new TelnetCommandExecutor(storageEngine);
    }

    @Override
    public byte[] apply(byte[] bytes, int offset, int length) throws Exception {
        String cmd = new String(bytes);
        logger.debug("Processing command: " + cmd);

        ByteArrayOutputStream normalOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errorOutputStream = new ByteArrayOutputStream();

        try (PrintStream outputPrintStream = new PrintStream(normalOutputStream, true, StandardCharsets.US_ASCII.toString());
             PrintStream errorPrintStream = new PrintStream(errorOutputStream, true, StandardCharsets.US_ASCII.toString())) {
            executor.executeCommand(cmd + "\r\n", outputPrintStream, errorPrintStream);
            String result = normalOutputStream.toString(StandardCharsets.US_ASCII.name());
            logger.debug("Command result: " + result);
            byte[] retBytes = result.getBytes(StandardCharsets.UTF_8);
            return retBytes;
        }
    }

    @Override
    public void readContentFrom(DataInput dataInput) throws Exception {
        logger.info("Recovering from snapshot, skip");
    }

    @Override
    public void writeContentTo(DataOutput dataOutput) throws Exception {
        logger.info("Storing snapshot, skip");
    }

    @Override
    public void roleChanged(Role role) {
        logger.info("Now the server is switched to role: " + role.name());
    }
}
