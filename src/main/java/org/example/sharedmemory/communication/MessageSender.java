package org.example.sharedmemory.communication;

import lombok.extern.slf4j.Slf4j;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

@Slf4j
public class MessageSender {
    public static void send(ProtoPayload.Message message, String receiverHost, int receiverPort) {
        log.info("Sent {} to {}", message.getNetworkMessage().getMessage().getType(), receiverPort);

        byte[] serializedMessage = message.toByteArray();

        try (Socket socket = new Socket(receiverHost, receiverPort);
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        ) {
            dataOutputStream.writeInt(serializedMessage.length);
            dataOutputStream.write(serializedMessage);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }
}
