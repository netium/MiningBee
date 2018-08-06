package org.ctp.network.telnet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import org.ctp.core.storageengine.IStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandler {
    private final Logger logger = LoggerFactory.getLogger(RequestHandler.class);
    private final static String CRLF = "\r\n";

    private StringBuilder unhandledInputString = new StringBuilder();
    private StringBuilder unsentOutputString = new StringBuilder();

    private final TelnetCommandExecutor executor;

    public RequestHandler(IStorageEngine storageEngine, SocketChannel channel) {
        executor = new TelnetCommandExecutor(storageEngine, channel);
    }

    public void sendWelcomeBanner(SocketChannel channel) {
        String command = "engine\r\n";
        byte[] commandBytes = command.getBytes(StandardCharsets.US_ASCII);
        ByteBuffer buffer = ByteBuffer.allocate(commandBytes.length);
        buffer.put(commandBytes);
        buffer.flip();
        handle(buffer, channel);
    }

    public void handle(ByteBuffer byteBuffer, SocketChannel channel) {
        if (byteBuffer.remaining() == 0)
            return;

        while(byteBuffer.remaining() > 0) {
            byte b = byteBuffer.get();
            unhandledInputString.append((char)b);
        }

        int crlfIndex = unhandledInputString.indexOf(CRLF);
        if (crlfIndex < 0)
            return;

        String command = unhandledInputString.substring(0, crlfIndex + CRLF.length());
        unhandledInputString.delete(0, crlfIndex + CRLF.length());
        if (command.isEmpty())
            return;

        logger.info("Handling incoming command: " + command);

        ByteArrayOutputStream normalOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errorOutputStream = new ByteArrayOutputStream();

        try (PrintStream outputPrintStream = new PrintStream(normalOutputStream, true, StandardCharsets.US_ASCII.toString());
        PrintStream errorPrintStream = new PrintStream(errorOutputStream, true, StandardCharsets.US_ASCII.toString())) {
            executor.executeCommand(command, outputPrintStream, errorPrintStream);
            byte[] outputBytes = normalOutputStream.toByteArray();
            ByteBuffer outputBuffer = ByteBuffer.allocate(outputBytes.length);
            outputBuffer.put(outputBytes);
            outputBuffer.flip();
            channel.write(outputBuffer);
        }
        catch (IOException e) {
            logger.error(e.toString());
            logger.error(e.getStackTrace().toString());
        }

        return;
    }
}
