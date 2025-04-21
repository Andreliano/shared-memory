package org.example.sharedmemory;

import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.domain.Process;
import org.example.sharedmemory.util.Util;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class SharedMemoryApplication {

    public static void main(String[] args) {
        SpringApplication.run(SharedMemoryApplication.class, args);
    }
}

@Component
class ProcessRunner implements CommandLineRunner {

    @Value("${hubHost}")
    private String hubHost;

    @Value("${hubPort}")
    private int hubPort;

    @Value("${processHost}")
    private String processHost;

    @Value("${processPorts}")
    private String processPortsStr;

    @Value("${processOwner}")
    private String processOwner;

    @Override
    public void run(String... args) throws InterruptedException {
        ProtoPayload.ProcessId hubInfo = ProtoPayload.ProcessId
                .newBuilder()
                .setHost(hubHost)
                .setPort(hubPort)
                .setOwner(Util.HUB_ID)
                .build();

        List<Integer> processesPorts = new ArrayList<>();
        Arrays.stream(processPortsStr.split(","))
                .map(Integer::parseInt)
                .forEach(processesPorts::add);

        List<ProtoPayload.ProcessId> processIds = processesPorts.stream()
                .map(port -> ProtoPayload.ProcessId.newBuilder()
                        .setHost(processHost)
                        .setPort(port)
                        .setOwner(processOwner)
                        .setIndex(processesPorts.indexOf(port) + 1)
                        .build())
                .toList();

        List<Thread> processes = new ArrayList<>();
        for (ProtoPayload.ProcessId protoProcessId : processIds) {
            Thread process = new Thread(new Process(protoProcessId, hubInfo), processHost + ":" + protoProcessId.getPort());
            process.start();
            processes.add(process);
        }

        for (Thread process : processes) {
            process.join();
        }
    }
}
