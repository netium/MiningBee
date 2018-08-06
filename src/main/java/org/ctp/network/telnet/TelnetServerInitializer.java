package org.ctp.network.telnet;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.ctp.core.storageengine.IStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelnetServerInitializer extends ChannelInitializer<SocketChannel> {
    private static final StringDecoder DECODER = new StringDecoder();
    private static final StringEncoder ENCODER = new StringEncoder();

    private Logger logger = LoggerFactory.getLogger(TelnetServerInitializer.class);

    private final IStorageEngine storageEngine;

    public TelnetServerInitializer(IStorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        logger.info("Initializing the channel from client: {}", socketChannel.remoteAddress());
        ChannelPipeline channelPipeline = socketChannel.pipeline();

        channelPipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
        channelPipeline.addLast(DECODER);
        channelPipeline.addLast(ENCODER);

        TelnetServerHandler serverHandler = new TelnetServerHandler(storageEngine);
        channelPipeline.addLast(serverHandler);
    }
}
