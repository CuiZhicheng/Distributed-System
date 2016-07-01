

import javafx.beans.property.IntegerProperty;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.lang.*;

/**
 * Created by dyc on 16-6-1.
 */
public class Manager{
    static private String ConfigurePath = "/home/czc/pagerank/master.config";
    //Signals
    static private int Ready2Next = 1;
    static private int Ready2Send = 2;
    static private int Recovery = 3;
    static private int Finish = 4;
    static private int MasterACK = 10;
    static private int ClientACK = 20;
    static private ServerSocket MasterServer = null;


    static public void FirstCheck(int myport, int workernumber, String[] addresses, int s, int lines) {
        int i;
        System.out.println(myport + " " + workernumber + " " + addresses[1]);
        boolean[] worker = new boolean[workernumber + 1];
        Arrays.fill(worker, false);
        String[] strs;

        try {
            boolean AllWorker = false;
            while (!AllWorker) {
                for (i = 1; i <= workernumber; i++) {
                    if (worker[i] == true)
                        continue;
                    strs = addresses[i].split(":");
                    String host = strs[0];
                    InetAddress localAddr1 = InetAddress.getByName(strs[0]);
                    int port = Integer.parseInt(strs[1]);
                    Socket master = new Socket(localAddr1, port);
                    if (!master.isConnected()) {
                        master.close();
                        continue;
                    }
                    DataOutputStream dos = new DataOutputStream(master.getOutputStream());
                    //0 for master, 1..n for workers
                    //start line ... end line
                    String msg = "0 " + s * (i - 1) + " " + Math.min(s * i, lines);
                    dos.writeUTF(msg);
                    //dos.close();
                    DataInputStream dis = new DataInputStream(master.getInputStream());
                    String str = dis.readUTF();
                    System.out.println(str);
                    strs = str.split(" ");
                    int workerNo = Integer.parseInt(strs[0]);
                    int signal = Integer.parseInt(strs[1]);
                    if (signal == ClientACK) {
                        worker[workerNo] = true;
                    }
                    dos.close();
                    dis.close();
                    master.close();
                    AllWorker = true;
                    for (i = 1; i <= workernumber; i++) {
                        AllWorker &= worker[i];
                    }
                }
                System.out.println("accept");

            }
            //server.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /*
    * Master Wait Signals From Workers
    * Once received, send back an ACK
    * #define Ready2Next 1
    * #define Ready2Send 2
    * #defien Recovery 3
    *
    */
    static public void WaitSignalFromWorker(int workernumber, String[] addresses, int Signal) {
        boolean[] barrier = new boolean[workernumber + 1];
        Socket[] WorkerSockets = new Socket[workernumber + 1];
        boolean HasCrashed = false;
        int[] CrashedWorker = new int[workernumber + 1];
        Arrays.fill(barrier, false);
        Arrays.fill(CrashedWorker, 0);
        boolean AllWorker = false;
        String str;
        String[] strs;
        int i;
        try {
            while (!AllWorker) {

                Socket master = MasterServer.accept();
                DataInputStream dis = new DataInputStream(master.getInputStream());
                str = dis.readUTF();
                //workerid signal
                strs = str.split(" ");
                int worker = Integer.parseInt(strs[0]);
                int signal = Integer.parseInt(strs[1]);
                if (signal == Recovery) {
                    System.out.println("Worker " + worker + " has been recovered.");
                    HasCrashed = true;
                    //continue;
                }

                WorkerSockets[worker] = master;
                barrier[worker] = true;
                AllWorker = true;
                for (i = 1; i <= workernumber; i++) {
                    AllWorker &= barrier[i];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // send ACK to all workers after all workers have reached the same point
        Socket MasterResponse = null;
        for (i = 1; i <= workernumber; i++) {
            try {
                MasterResponse = WorkerSockets[i];
                DataOutputStream dos = new DataOutputStream(MasterResponse.getOutputStream());
                if (HasCrashed || Signal == Finish)
                    str = "0 " + String.valueOf(Signal);
                else
                    str = "0 " + String.valueOf(MasterACK);
                dos.writeUTF(str);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) {
        String filePath = null;
        String ConfigurePath = "/home/czc/pagerank/master.config";
        int myport = 0;
        int workernumber = 0;
        int TimesLimit = 0;
        int Times = 0;
        int k;
        int i;
        String[] addresses = {"127.0.0.1:10081", "127.0.0.1:10082"};;

        /*
        * Configure File
        * Master port
        * WorkerNumber
        * Following WorkerNumber Lines:
        *      Worker Address:Port
        * FilePath
        * Calculation Times Limit;
        */
        System.out.println("Begin read Configure File");
        try {
            File file = new File(ConfigurePath);
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            tempString = reader.readLine();
            myport = Integer.parseInt(tempString);
            MasterServer = new ServerSocket(myport);
            tempString = reader.readLine();
            workernumber = Integer.parseInt(tempString);
            addresses = new String[workernumber + 1];
            for (i = 1; i <= workernumber; i++) {
                tempString = reader.readLine();
                addresses[i] = tempString;
            }
            tempString = reader.readLine();
            filePath = tempString;
            tempString = reader.readLine();
            TimesLimit = Integer.parseInt(tempString);

        } catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("End read Configure File\n");

        HashMap<String, ArrayList<String> > table = DataTool.readOriginFile(filePath);

        int FileLines = table.size();
        int s = table.size() / workernumber;
        System.out.println(s);
        //First Check
        System.out.println("Begin First Check.");
        FirstCheck(myport, workernumber, addresses, s, FileLines);
        System.out.println("First Check Finished.");

        boolean ff = true;
        while(ff) {
            if (Times < TimesLimit) {
                System.out.println("Wait for all Workers to be ready to start calculation");
                WaitSignalFromWorker(workernumber, addresses, Ready2Next);
                System.out.println("Calculation " + Times + " begins.");
            } else {
                System.out.println("Calculation Finished");
                WaitSignalFromWorker(workernumber, addresses, Finish);
                try{
                    Thread.currentThread().sleep(1000);
                }catch(InterruptedException ie){
                    ie.printStackTrace();
                }
                break;
            }

            //wait 1s for workers to calculate

            try{
                Thread.currentThread().sleep(1000);
            }catch(InterruptedException ie){
                ie.printStackTrace();
            }


            //wait for workers
            //Ready2Send
            System.out.println("Wait for all Workers to be ready to send data.");
            WaitSignalFromWorker(workernumber, addresses, Ready2Send);
            System.out.println("Transmission " + Times + " begins.");
            Times++;
        }
    }
}
