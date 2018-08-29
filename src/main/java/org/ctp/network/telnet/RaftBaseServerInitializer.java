package org.ctp.network.telnet;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.jgroups.raft.RaftHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftBaseServerInitializer extends ChannelInitializer<SocketChannel> {
    private static final StringDecoder DECODER = new StringDecoder();
    private static final StringEncoder ENCODER = new StringEncoder();

    private Logger logger = LoggerFactory.getLogger(RaftBaseServerInitializer.class);

    private final RaftHandle raftHandle;

    public RaftBaseServerInitializer(RaftHandle raftHandle) {
        this.raftHandle = raftHandle;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        logger.info("Initializing the channel from client: {}", socketChannel.remoteAddress());
        ChannelPipeline channelPipeline = socketChannel.pipeline();

        channelPipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
        channelPipeline.addLast(DECODER);
        channelPipeline.addLast(ENCODER);

        RaftBaseServerHandler serverHandler = new RaftBaseServerHandler(raftHandle);
        channelPipeline.addLast(serverHandler);
    }

}
