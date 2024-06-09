/* FileCopyClient.java
 Version 1.2 - tracking
 Praktikum 3 Rechnernetze BAI4 HAW Hamburg
 Autoren:
 */

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class FileCopyClient extends Thread {

  // -------- Constants
  public final static boolean TEST_OUTPUT_MODE = false;
  public final int SERVER_PORT = 23000;
  public final int UDP_PACKET_SIZE = 1008;
  public final int HEADER_SIZE = 8;

  // -------- Public parms
  public String servername;
  public String sourcePath;
  public String destPath;
  public int windowSize;
  public long serverErrorRate;

  // -------- Variables
  private DatagramSocket clientSocket;
  private InetAddress serverAddress;
  private int sendBase = 1; // Start after the control packet
  private int nextSeqNum = 1; // Start after the control packet
  private long timeoutValue = 100000000L; // nanoseconds
  private Map<Long, FCpacket> sendBuffer;
  private Queue<FCpacket> ackQueue;
  private long estimatedRTT = 100000000L; // initial RTT estimation
  private long devRTT = 0; // deviation of RTT
  private boolean transferComplete = false;
  private AckReceiver ackReceiver;
  private final Object lock = new Object(); // Synchronization lock

  // -------- Tracking
  private boolean outputTrackingInfo = true;
  private long startTime;
  private long totalTimeForFile;
  private int totalPacketsSentMultipleTimes = 0;
  private int totalReceivedAckConfirmations = 0;
  private long totalRTT = 0;
  private int ackCount = 0;

  // Constructor
  public FileCopyClient(String serverArg, String sourcePathArg,
                        String destPathArg, String windowSizeArg, String errorRateArg) {
    servername = serverArg;
    sourcePath = sourcePathArg;
    destPath = destPathArg;
    windowSize = Integer.parseInt(windowSizeArg);
    serverErrorRate = Long.parseLong(errorRateArg);
  }

  public void runFileCopyClient() {
    startTime = System.currentTimeMillis();
    try {
      clientSocket = new DatagramSocket();
      serverAddress = InetAddress.getByName(servername);
      sendBuffer = new ConcurrentHashMap<>();
      ackQueue = new ConcurrentLinkedQueue<>();

      // Send control packet and wait for acknowledgment
      FCpacket controlPacket = makeControlPacket();
      sendPacket(controlPacket);
      waitForControlAck(controlPacket);

      ackReceiver = new AckReceiver();
      ackReceiver.start();

      try (FileInputStream fileInputStream = new FileInputStream(sourcePath)) {
        byte[] fileBuffer = new byte[UDP_PACKET_SIZE - HEADER_SIZE];
        int bytesRead;

        while ((bytesRead = fileInputStream.read(fileBuffer)) != -1) {
          synchronized (lock) {
            while (nextSeqNum >= sendBase + windowSize) {
              lock.wait(); // Wait if the window is full
            }
          }

          FCpacket packet = new FCpacket(nextSeqNum, fileBuffer, bytesRead);
          sendPacket(packet);
          nextSeqNum++;
        }

        // wait for all packets to be acknowledged
        synchronized (lock) {
          while (sendBase < nextSeqNum) {
            lock.wait(); // Wait for signal from AckReceiver
          }
        }

        // Indicate that the transfer is complete
        transferComplete = true;
      }

      // Stop tracking
      totalTimeForFile = System.currentTimeMillis() - startTime;

      // Close the socket to unblock the AckReceiver thread
      clientSocket.close();

      // Wait for the AckReceiver to finish
      ackReceiver.join();

      if (outputTrackingInfo) {
        printTrackingInfo();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void waitForControlAck(FCpacket controlPacket) throws IOException {
    byte[] receiveData = new byte[UDP_PACKET_SIZE];
    while (true) {
      DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
      clientSocket.receive(receivePacket);
      FCpacket ackPacket = new FCpacket(receivePacket.getData(), receivePacket.getLength());

      long ackNum = ackPacket.getSeqNum();
      if (ackNum == controlPacket.getSeqNum()) {
        controlPacket.setValidACK(true);
        cancelTimer(controlPacket);
        break;
      }
    }
  }

  public void sendPacket(FCpacket packet) throws IOException {
    byte[] sendData = packet.getSeqNumBytesAndData();
    DatagramPacket udpPacket = new DatagramPacket(sendData, sendData.length, serverAddress, SERVER_PORT);
    clientSocket.send(udpPacket);
    packet.setTimestamp(System.nanoTime());
    sendBuffer.put(packet.getSeqNum(), packet);
    if (packet.getSeqNum() != 0 || packet.getTimer() == null) {
      startTimer(packet);
    }
  }

  public void startTimer(FCpacket packet) {
    FC_Timer timer = new FC_Timer(timeoutValue, this, packet.getSeqNum());
    packet.setTimer(timer);
    timer.start();
  }

  public void cancelTimer(FCpacket packet) {
    if (packet.getTimer() != null) {
      packet.getTimer().interrupt();
    }
  }

  public void timeoutTask(long seqNum) {
    try {
      FCpacket packet = sendBuffer.get(seqNum);
      if (packet != null && !packet.isValidACK()) {
        sendPacket(packet);
        totalPacketsSentMultipleTimes++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void computeTimeoutValue(long sampleRTT) {
    double alpha = 0.125;
    double beta = 0.25;
    estimatedRTT = (long) ((1 - alpha) * estimatedRTT + alpha * sampleRTT);
    devRTT = (long) ((1 - beta) * devRTT + beta * Math.abs(sampleRTT - estimatedRTT));
    timeoutValue = estimatedRTT + 4 * devRTT;
  }

  public FCpacket makeControlPacket() {
    String sendString = destPath + ";" + windowSize + ";" + serverErrorRate;
    byte[] sendData = null;
    try {
      sendData = sendString.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return new FCpacket(0, sendData, sendData.length);
  }

  public void testOut(String out) {
    if (TEST_OUTPUT_MODE) {
      System.err.printf("%,d %s: %s\n", System.nanoTime(), Thread.currentThread().getName(), out);
    }
  }

  private void printTrackingInfo() {
    System.out.println("Tracking Information:");
    System.out.println("Total time for file transfer: " + totalTimeForFile + " ms");
    System.out.println("Total packets sent multiple times: " + totalPacketsSentMultipleTimes);
    System.out.println("Total received ACK confirmations: " + totalReceivedAckConfirmations);
    System.out.println("Average RTT of all ACKs: " + (ackCount > 0 ? totalRTT / ackCount : 0) + " ns");
  }

  private class AckReceiver extends Thread {
    public void run() {
      try {
        byte[] receiveData = new byte[UDP_PACKET_SIZE];
        while (true) {
          DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
          try {
            clientSocket.receive(receivePacket);
          } catch (SocketException e) {
            // Exit if socket is closed
            if (clientSocket.isClosed()) break;
          }
          FCpacket ackPacket = new FCpacket(receivePacket.getData(), receivePacket.getLength());

          long ackNum = ackPacket.getSeqNum();
          FCpacket packet = sendBuffer.get(ackNum);

          if (packet != null) {
            cancelTimer(packet);
            packet.setValidACK(true);
            long sampleRTT = System.nanoTime() - packet.getTimestamp();
            computeTimeoutValue(sampleRTT);

            synchronized (lock) {
              if (ackNum == sendBase) {
                while (sendBuffer.containsKey(sendBase) && sendBuffer.get(sendBase).isValidACK()) {
                  sendBuffer.remove(sendBase);
                  sendBase++;
                }
              }
              lock.notifyAll(); // Notify main thread
            }

            totalReceivedAckConfirmations++;
            totalRTT += sampleRTT;
            ackCount++;
          }

          if (transferComplete && sendBuffer.isEmpty()) {
            break;
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] argv) throws Exception {
    if (argv.length != 5) {
      System.out.println("Usage: java FileCopyClient <Server> <SourcePath> <DestPath> <WindowSize> <ErrorRate>");
      System.exit(1);
    }
    FileCopyClient myClient = new FileCopyClient(argv[0], argv[1], argv[2], argv[3], argv[4]);
    myClient.outputTrackingInfo = true; // Enable tracking output
    myClient.runFileCopyClient();
  }
}
