package dev.pig.obrc.pipeline;

import java.io.IOException;

public class Git {

    static String commitMessage() throws IOException, InterruptedException {
        return exec("git log -1 pretty=%s");
    }

    static String commitHash() throws IOException, InterruptedException {
        return exec("git rev-parse --short HEAD");
    }

    private static String exec(String cmd) throws IOException, InterruptedException {
        Process process = new ProcessBuilder("bash", "-c", cmd).start();
        process.waitFor();
        return new String(process.getInputStream().readAllBytes()).trim();
    }

}
