package org.example.consensus.communication;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;

@Slf4j
@Setter
@RequiredArgsConstructor
public class MessageReceiver implements Runnable {
    private final int processPort;
    private final BlockingQueue<ProtoPayload.Message> messageQueue;
    private boolean shouldStop = false;

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(processPort)) {
            serverSocket.setSoTimeout(5000);
            log.info("Waiting for requests");
            while (!shouldStop) {
                receiveMessage(serverSocket);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private void receiveMessage(ServerSocket serverSocket) throws IOException {
        try (Socket clientSocket = serverSocket.accept();
             DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream())
        ) {
            int messageSize = dataInputStream.readInt();
            byte[] byteBuffer = new byte[messageSize];
            int readMessageSize = dataInputStream.read(byteBuffer, 0, messageSize);

            if (messageSize != readMessageSize) {
                log.error("Received message doesn't have the expected size: expected={} | actual={}", messageSize, readMessageSize);
                return;
            }

            ProtoPayload.Message message = ProtoPayload.Message.parseFrom(byteBuffer);

            if (!ProtoPayload.Message.Type.NETWORK_MESSAGE.equals(message.getType())) {
                log.error("The message received isn't a network message: actual={}", message.getType());
                return;
            }

            log.info("Received {} from {}", message.getNetworkMessage().getMessage().getType(), message.getNetworkMessage().getSenderListeningPort());
            messageQueue.add(message);
        } catch (SocketTimeoutException ignored) {
        }
    }

}
