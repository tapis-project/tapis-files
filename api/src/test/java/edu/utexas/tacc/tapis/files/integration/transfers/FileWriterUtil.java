package edu.utexas.tacc.tapis.files.integration.transfers;

import java.io.File;
import java.io.;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileWriterUtil {

    public static void main(String[] args) throws Exception {
        int size = 1000;
        int count = 10;
        boolean alphaNumericOnly = false;
        String basePath = "/tmp/integrationTestRandomFiles";
        String filePrefixToUse = "integration_test_file_";

        for(int i=0;i<args.length;i++) {
            String nextArg = args[i];
            try {
                switch (nextArg) {
                    case "-s":
                    case "--size":
                        size = Integer.parseInt(args[i + 1]);
                        i++;
                        break;

                    case "-c":
                    case "--count":
                        count = Integer.parseInt(args[i + 1]);
                        i++;
                        break;

                    case "-a":
                    case "--alphaNumeric":
                        alphaNumericOnly = true;
                        break;

                    default:
                        throw new RuntimeException("Unknown argument: " + nextArg);
                }
            } catch (Exception ex) {
                System.out.println("Exception: " + ex.getMessage());
                usage(System.out);
            }
        }

        File outputDirectory = new File(basePath);
        if(!outputDirectory.exists()) {
            outputDirectory.mkdirs();
        }

        if(outputDirectory.exists() && !outputDirectory.isDirectory()) {
            throw new RuntimeException("outputDirectory exists, but is not a directory");
        }

        ExecutorService executorService = Executors.newFixedThreadPool(1);

        for(int i=0;i<count;i++) {
            Path destinationPath = Path.of(basePath, filePrefixToUse + UUID.randomUUID());
            File outputFile = destinationPath.toFile();
            FileOutputStream outputStream = new FileOutputStream(outputFile);
            RandomStreamWriter writer = new RandomStreamWriter(size, alphaNumericOnly, IntegrationTestUtils.SHA256);
            InputStream inputStream = writer.initInputStream();
            executorService.submit(writer);
            inputStream.transferTo(outputStream);
            System.out.println("Sha255 for file : " + outputFile.getName() + ":" + writer.getDigest());
        }

        executorService.shutdown();
    }

    public static void usage(PrintStream out) {
        out.println("Usage:  FileWriterUtil [-s|--size <size>] [-c|--count <filesToWrite>] [-a|--alphaNumeric]");
    }
}
