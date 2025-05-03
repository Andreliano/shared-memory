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
        ProtoPayload.ProcessId hubInfo = buildHubInfo();

        List<Integer> processPorts = parseProcessPorts(processPortsStr);
        List<ProtoPayload.ProcessId> processIds = buildProcessIds(processPorts);

        List<Thread> processThreads = startProcessThreads(processIds, hubInfo);

        waitForProcesses(processThreads);
    }

    private ProtoPayload.ProcessId buildHubInfo() {
        return ProtoPayload.ProcessId.newBuilder()
                .setHost(hubHost)
                .setPort(hubPort)
                .setOwner(Util.HUB_ID)
                .build();
    }

    private List<Integer> parseProcessPorts(String portsStr) {
        return Arrays.stream(portsStr.split(","))
                .map(Integer::parseInt)
                .toList();
    }

    private List<ProtoPayload.ProcessId> buildProcessIds(List<Integer> ports) {
        return ports.stream()
                .map(port -> ProtoPayload.ProcessId.newBuilder()
                        .setHost(processHost)
                        .setPort(port)
                        .setOwner(processOwner)
                        .setIndex(ports.indexOf(port) + 1)
                        .build())
                .toList();
    }

    private List<Thread> startProcessThreads(List<ProtoPayload.ProcessId> processIds, ProtoPayload.ProcessId hubInfo) {
        List<Thread> threads = new ArrayList<>();
        for (ProtoPayload.ProcessId processId : processIds) {
            Thread thread = new Thread(new Process(processId, hubInfo), processHost + ":" + processId.getPort());
            thread.start();
            threads.add(thread);
        }
        return threads;
    }

    private void waitForProcesses(List<Thread> threads) throws InterruptedException {
        for (Thread thread : threads) {
            thread.join();
        }
    }

}
