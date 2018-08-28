package org.ctp.core.storageengine.lsm;

import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.network.telnet.TelnetCommandExecutor;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.protocols.raft.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;

public class ZeusStateMachine implements StateMachine, RAFT.RoleChange {
    private final Logger logger = LoggerFactory.getLogger(ZeusStateMachine.class);

    private final IStorageEngine storageEngine;
    private final TelnetCommandExecutor executor;

    public ZeusStateMachine(final IStorageEngine storageEngine) {
        this.storageEngine = storageEngine;
        this.executor = new TelnetCommandExecutor(storageEngine);
    }

    @Override
    public byte[] apply(byte[] bytes, int offset, int length) throws Exception {
        String s = new String(bytes);
        System.out.println("Receive command: s");
        return new byte[0];
    }

    @Override
    public void readContentFrom(DataInput dataInput) throws Exception {
        logger.info("Recovering from snapshot, skip");
    }

    @Override
    public void writeContentTo(DataOutput dataOutput) throws Exception {
        logger.info("Storing sanpshot, skip");
    }

    @Override
    public void roleChanged(Role role) {
        logger.info("Now the server is switched to role: " + role.name());
    }
}
