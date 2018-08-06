package org.ctp.network.telnet;

import org.ctp.core.storageengine.IStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class TelnetBaseNetworkServer implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(TelnetBaseNetworkServer.class);

    private final static int SERVER_PORT = 18889;
    private IStorageEngine storageEngine = null;

    private ServerSocketChannel serverSocketChannel = null;

    private Selector selector;

    public TelnetBaseNetworkServer(IStorageEngine storageEngine) throws IOException {
        this.storageEngine = storageEngine;

        selector = Selector.open();
    }

    @Override
    public void run() {
        try {
            initServer();

            while (true) {
                int readyChannels = 0;
                try {
                    readyChannels = selector.select();
                    if (readyChannels == 0)
                        continue;

                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> iterator = selectedKeys.iterator();

                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();

                        if(key.isAcceptable()) {
                            processAcceptRequest(key);
                        } else if (key.isConnectable()) {
                            // a connection was established with a remote server.
                        } else if (key.isReadable()) {
                            processInputRequest(key);
                        } else if (key.isWritable()) {
                            // a channel is ready for writing
                        }

                        iterator.remove();
                    }
                }
                catch (IOException e) {
                    logger.error(e.toString());
                    logger.error(e.getStackTrace().toString());
                }
            }

            // closeServer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initServer() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(SERVER_PORT));
        serverSocketChannel.configureBlocking(false);

        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        logger.info("Open port: " + SERVER_PORT + " to accepting client request...");
    }

    private void acceptIncommingConnection() throws IOException {
        while (true) {
            SocketChannel socketChannel = serverSocketChannel.accept();
        }
    }

    private void closeServer() throws IOException {
        serverSocketChannel.close();
    }

    private void processInputRequest(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel)key.channel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        channel.read(byteBuffer);
        byteBuffer.flip();

        RequestHandler handler = (RequestHandler)key.attachment();
        handler.handle(byteBuffer, channel);
    }

    public void processAcceptRequest(SelectionKey key) throws IOException {
        ServerSocketChannel channel = (ServerSocketChannel)key.channel();
        SocketChannel socketChannel = channel.accept();
        logger.info("Accepted incoming request from client: " + socketChannel.socket().getRemoteSocketAddress().toString());
        socketChannel.configureBlocking(false);
        RequestHandler handler = new RequestHandler(storageEngine, socketChannel);
        socketChannel.register(selector, SelectionKey.OP_READ, handler);
        handler.sendWelcomeBanner(socketChannel);
    }
}
