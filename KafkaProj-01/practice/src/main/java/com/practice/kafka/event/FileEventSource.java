package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    public final boolean keepRunning = true;
    private long filePointer = 0;

    private final long updateInterval;
    private final File file;
    private final EventHandler eventHandler;

    public FileEventSource(long updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            while (keepRunning) {
                Thread.sleep(updateInterval);
                // file 크기 계산
                long length = file.length();
                if (length < filePointer) {
                    logger.info("log file was reset as filePointer is longer than filePointer");
                    filePointer = length;
                } else if (length > filePointer) {
                    readAppendAndSend();
                }
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private void readAppendAndSend() throws ExecutionException, IOException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        raf.seek(filePointer);
        String line;
        while ((line = raf.readLine()) != null) {
            sendMessage(line);
        }
        filePointer = raf.getFilePointer();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        final String delimiter = ",";
        String[] tokens = line.split(delimiter);
        String key = tokens[0];
        StringBuffer value = new StringBuffer();

        for (int i = 1; i < tokens.length; i++) {
            if (i != tokens.length - 1) {
                value.append(tokens[i] + delimiter);
            } else {
                value.append(tokens[i]);
            }
        }
        MessageEvent messageEvent = new MessageEvent(key, value.toString());
        eventHandler.onMessage(messageEvent);
    }
}
