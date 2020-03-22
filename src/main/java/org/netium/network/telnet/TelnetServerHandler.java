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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.netium.server.storageengine.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class TelnetServerHandler extends SimpleChannelInboundHandler<String> {
    private final Logger logger = LoggerFactory.getLogger(TelnetServerHandler.class);

    private final TelnetCommandExecutor executor;

    public TelnetServerHandler(StorageEngine storageEngine) {
        executor = new TelnetCommandExecutor(storageEngine);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ByteArrayOutputStream normalOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errorOutputStream = new ByteArrayOutputStream();

        try (PrintStream outputPrintStream = new PrintStream(normalOutputStream, true, StandardCharsets.US_ASCII.toString());
             PrintStream errorPrintStream = new PrintStream(errorOutputStream, true, StandardCharsets.US_ASCII.toString())) {
            executor.executeCommand("engine\r\n", outputPrintStream, errorPrintStream);
            String result = normalOutputStream.toString(StandardCharsets.US_ASCII.name());
            ChannelFuture future = ctx.writeAndFlush(result);
        }
        catch (IOException e) {
            logger.error(e.toString());
            logger.error(e.getStackTrace().toString());
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String s) throws Exception {
        ByteArrayOutputStream normalOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errorOutputStream = new ByteArrayOutputStream();

        try (PrintStream outputPrintStream = new PrintStream(normalOutputStream, true, StandardCharsets.US_ASCII.toString());
             PrintStream errorPrintStream = new PrintStream(errorOutputStream, true, StandardCharsets.US_ASCII.toString())) {
            executor.executeCommand(s + "\r\n", outputPrintStream, errorPrintStream);
            String result = normalOutputStream.toString(StandardCharsets.US_ASCII.name());
            ChannelFuture future = ctx.writeAndFlush(result);
            if (executor.isClientWantExiting()) {
                future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                        logger.info("Close the connection to client: {}", ctx.channel().remoteAddress());
                        ctx.close();
                    }
                });
            }
        }
        catch (IOException e) {
            logger.error(e.toString());
            logger.error(e.getStackTrace().toString());
        }
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
}
