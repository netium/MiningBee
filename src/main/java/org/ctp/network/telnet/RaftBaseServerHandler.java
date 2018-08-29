package org.ctp.network.telnet;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jgroups.raft.RaftHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class RaftBaseServerHandler extends SimpleChannelInboundHandler<String> {
    private final Logger logger = LoggerFactory.getLogger(RaftBaseServerHandler.class);

    private RaftHandle raftHandle;

    public RaftBaseServerHandler(final RaftHandle raftHandle) {
        this.raftHandle = raftHandle;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        final String command = "engine";
        executeCommand(command, ctx);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String s) throws Exception {
        executeCommand(s, ctx);
    }

    private void executeCommand(String command, ChannelHandlerContext ctx) throws Exception {
        command = command + "\r\n";
        final byte[] bytes = command.getBytes(StandardCharsets.UTF_8);

        final byte[] returnBytes = raftHandle.set(bytes, 0, bytes.length, 10, TimeUnit.SECONDS);
        final String result = new String(returnBytes, StandardCharsets.UTF_8);
        ChannelFuture future = ctx.writeAndFlush(result);
    }
}
