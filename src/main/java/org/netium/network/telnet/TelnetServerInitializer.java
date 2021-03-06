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

package org.netium.network.telnet;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.netium.server.storageengine.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelnetServerInitializer extends ChannelInitializer<SocketChannel> {
    private static final StringDecoder DECODER = new StringDecoder();
    private static final StringEncoder ENCODER = new StringEncoder();

    private Logger logger = LoggerFactory.getLogger(TelnetServerInitializer.class);

    private final StorageEngine storageEngine;

    public TelnetServerInitializer(StorageEngine storageEngine) {
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
