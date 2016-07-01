//import com.sun.xml.internal.ws.api.pipe.FiberContextSwitchInterceptor;
//import com.sun.xml.internal.ws.encoding.StringDataContentHandler;

import javax.print.attribute.standard.MediaSize;
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
public class Worker1 extends Thread{
    static private String ConfigurePath = "/home/czc/pagerank/worker1.config";
    //Signals
    static private int Ready2Next = 1;
    static private int Ready2Send = 2;
    static private int Recovery = 3;
    static private int Finish = 4;
    static private int MasterACK = 10;
    static private int ClientACK = 20;

    private int WorkerId = 0;
    private int WorkerNumber = 0;
    private InetAddress localAddr = null;
    private int myport = 0;
    private String MasterAddress = null;
    private String[] WorkerAddresses = null;
    private String FilePath = null;
    private String CheckPointPath0 = null;
    private String CheckPointPath1 = null;
    private ServerSocket WorkerServer = null;

    private int Count = 0;
    private int StartLine = 0;
    private int EndLine = 0;
    private int CrashWorker = -1;
    private HashMap<String, ArrayList<String> > table = null;
    private HashMap<String, Double> initialVector = null;
    private Set<String> Variable = null;
    private HashMap<String, Double> current = null;
    private HashMap<String, Double>[] OtherWorkerData = null;

    /* ReadConfigure
    * Read client.config to load static data and configuration
    * WorkerId
    * MasterAddress (address:port)
    * WorkerNumber
    * Following WorkerNumber Line:
    *   worker0(address:port)
    *   worker1(address:port)
    *   ...
    * FilePath
    * CheckPointPath0
    * CheckPointPath1
    */
    public void ReadConfigure() {
        int i;
        try {
            File file = new File(ConfigurePath);
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            tempString = reader.readLine();
            WorkerId = Integer.parseInt(tempString);
            tempString = reader.readLine();
            MasterAddress = tempString;
            tempString = reader.readLine();
            WorkerNumber = Integer.parseInt(tempString);
            WorkerAddresses = new String[WorkerNumber + 1];
            for (i = 1; i <= WorkerNumber; i++) {
                tempString = reader.readLine();
                WorkerAddresses[i] = tempString;
            }
            String[] strs = WorkerAddresses[WorkerId].split(":");
            localAddr = InetAddress.getByName(strs[0]);
            myport = Integer.parseInt(strs[1]);
            tempString = reader.readLine();
            FilePath = tempString;
            tempString = reader.readLine();
            CheckPointPath0 = tempString;
            tempString = reader.readLine();
            CheckPointPath1 = tempString;
            reader.close();
            OtherWorkerData = new HashMap[WorkerNumber + 1];
            System.out.println(WorkerId + "!" + MasterAddress + "!" + WorkerNumber + " " + WorkerAddresses[1]
                    + " " + FilePath + " " + CheckPointPath0 + " " + CheckPointPath1);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /*
    * Master calls FirstCheck to all Workers, msg:
    *  MasterId(0) + " " + StartLine + " " +  EndLine + "\n";
    *  Worker records StartLine and EndLine
    *  Then send ClientACK to Master
    */
    public void AnswerFirstCheck() {
        try {
            WorkerServer = new ServerSocket(myport);
        } catch(Exception e) {
            e.printStackTrace();
        }
        String SelfAddress = WorkerAddresses[WorkerId];
        String[] master = MasterAddress.split(":");
        String[] self = SelfAddress.split(":");
        String masterhost = master[0];
        int masterport = Integer.parseInt(master[1]);
        String selfhost = self[0];
        int selfport = Integer.parseInt(self[1]);
        try {
            //ServerSocket server = new ServerSocket(selfport);
            while (true) {
                Socket worker = WorkerServer.accept();
                DataInputStream dis = new DataInputStream(worker.getInputStream());
                String str = dis.readUTF();
                System.out.println(str);
                String[] msg = str.split(" ");
                int msgid = Integer.parseInt(msg[0]);
                if (msgid == 0) {
                    StartLine = Integer.parseInt(msg[1]);
                    EndLine = Integer.parseInt(msg[2]);
                    System.out.println(msg[1] + " " + msg[2]);
                    System.out.println(worker.getLocalPort());
                    DataOutputStream dos = new DataOutputStream(worker.getOutputStream());
                    String response = String.valueOf(WorkerId) + " " + String.valueOf(ClientACK);
                    System.out.println(response);
                    dos.writeUTF(response);
                    dis.close();
                    dos.close();
                    worker.close();
                    //server.close();
                    break;
                }
                dis.close();
                worker.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* CheckPoint
    *  CheckPoint0 or CheckPoint1
    *  0 = CheckPoint before next Calculation
    *  1 = CheckPoint after Calculation but before Transmission
    *  if not exists, first iteration, create file, return 0
    *  if  exists compare count
    *  cout = Local Iteration Count, Compare with Count writen in the CheckPoint
    *  if  equal, all right, return 1;
    *  if not equal, crash recovery, return 2;
    */
    public int CheckPoint(int Id, int count) {
        String CPpath = null;
        if (Id == 0)
            CPpath = CheckPointPath0;
        else
            CPpath = CheckPointPath1;
        try {
            File file = new File(CPpath);
            if (!file.exists()) {
                return 0;
            }
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader(file));
            String str = null;
            str = reader.readLine();
            reader.close();
            int CPCount = Integer.parseInt(str);
            if (CPCount == count)
                return 1;
            return 2;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /*
    * Send Signal to Master
    * Ready2Next, Ready2Send, Recovery
    */
    public int Send2Master(int signal) {
        String strs[] = MasterAddress.split(":");
        String host = strs[0];
        Socket worker = null;
        int port = Integer.parseInt(strs[1]);
        try {
            worker = new Socket(host, port);
            DataOutputStream dos = new DataOutputStream(worker.getOutputStream());
            String msg = String.valueOf(WorkerId) + " " + String.valueOf(signal);
            dos.writeUTF(msg);
            DataInputStream dis = new DataInputStream(worker.getInputStream());
            msg = dis.readUTF();
            String[] res = msg.split(" ");
            int Id = Integer.parseInt(res[0]);
            int Sig = Integer.parseInt(res[1]);
            //dis.close();
            if (Sig == Finish) {
                worker.close();
                return -1;
            }
            if (Sig == MasterACK) {
                worker.close();
                return 0;
            }
            if (Sig == Ready2Next) {
                worker.close();
                return 1;
            }
            if (Sig == Ready2Send) {
                worker.close();
                return 2;
            }
            //return worker;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /*
    * Send2Master Then WaitSignalFromMaster
    * if ACK return 0
    * if Recovery (someone crashed)
    *   if signal sent before is Ready2Next (Sending and Aggregating data)
    *       return 1;
    *       Redo Sending and Aggregating data
    *   if signal sent before is Ready2Send (Calculating)
    *       return 2;
    *       Redo Calculation
    */

    public int WaitSignalFromMaster(Socket socket){
        String[] strs = WorkerAddresses[WorkerId].split(":");
        int port = Integer.parseInt(strs[1]);
        int signal = 0;
        Socket worker = socket;
        try {
            //ServerSocket server = new ServerSocket(port);
            DataInputStream dis = new DataInputStream(worker.getInputStream());
            String msg = dis.readUTF();
            String[] res = msg.split(" ");
            int Id = Integer.parseInt(res[0]);
            int Sig = Integer.parseInt(res[1]);
            //dis.close();
            if (Sig == Finish) {
                worker.close();
                //server.close();
                return -1;
            }
            if (Sig == MasterACK) {
                worker.close();
                //server.close();
                return 0;
            }
            if (Sig == Ready2Next) {
                worker.close();
                //server.close();
                return 1;
            }
            if (Sig == Ready2Send) {
                worker.close();
                //server.close();
                return 2;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /*
    * First receive data from Worker whose WorkerId < local WorkerId
    * Then send local data to the Worker
    * When all Workers (WorkerId < local WorkerId) have finished transmission
    * local Worker begin to send local data to Workers whose WorkerId > local WorkerId
    * Then receive data from those Workers (WorkerId > local WorkerId)
    * other Workers' data are recorded in the array OtherWorkerData
    * */
    public void SendDataToWorkers(HashMap<String, Double> current) {
        int i;

        Vector<String> output = new Vector<>();
        boolean[] allworker = new boolean[WorkerId + 1];
        Arrays.fill(allworker, false);
        allworker[WorkerId - 1] = true;
        int len = current.entrySet().size();
        System.out.println("len is " + len);
        int time = len / 1000;
        System.out.println("time is " + time);
        int step, it;
        step = it = 0;
        String tmp, cur;
        tmp = String.valueOf(WorkerId) + ",";
        cur = tmp + String.valueOf(it);
        for (Map.Entry<String, Double> entry : current.entrySet()) {
            if (step == 1000) {
                output.add(cur);
                step = 0;
                System.out.println("Iteration " + it);
                it++;
                if (it == time)
                    it = -1;
                cur = tmp + String.valueOf(it);
            }
            cur += "," + entry.getKey() + "," + entry.getValue();
            step++;
        }
        System.out.println("Iteration " + it);
        output.add(cur);
        try {
            boolean all = false;
            if (WorkerId == 1) all = true;
            while (!all) {
                Socket worker = WorkerServer.accept();
                DataInputStream dis = new DataInputStream(worker.getInputStream());
                DataOutputStream dos = new DataOutputStream(worker.getOutputStream());
                String msg;
                msg = dis.readUTF();
                String[] input = msg.split(",");
                int otherworker = Integer.parseInt(input[0]);
                int status = Integer.parseInt(input[1]);
                allworker[otherworker] = true;
                OtherWorkerData[otherworker] = new HashMap<>();
                System.out.println("Status: " + status);
                while (status != -1) {
                    for (i = 2; i < input.length; i += 2) {
                        String key = input[i];
                        Double value = Double.parseDouble(input[i + 1]);
                        OtherWorkerData[otherworker].put(key, value);
                    }
                    System.out.println("Length " + input.length / 2);
                    msg = dis.readUTF();
                    input = msg.split(",");
                    status = Integer.parseInt(input[1]);
                }
                for (i = 2; i < input.length; i += 2) {
                    String key = input[i];
                    Double value = Double.parseDouble(input[i + 1]);
                    OtherWorkerData[otherworker].put(key, value);
                }
                System.out.println("Length " + input.length / 2);
                System.out.println("Receive Data From " + otherworker + " With Length: " + OtherWorkerData[otherworker].size());
                for (String s : output)
                    dos.writeUTF(s);

                //dos.close();
                //dis.close();
                //worker.close();
                all = true;
                for (i = 1; i < WorkerId; i++) {
                    all &= allworker[i];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (i = WorkerId + 1; i <= WorkerNumber; i++) {
            String[] other = WorkerAddresses[i].split(":");
            String otherhost = other[0];
            int otherport = Integer.parseInt(other[1]);
            System.out.println(otherhost + " " + otherport);
            try {
                Socket localworker = new Socket(otherhost, otherport);
                DataInputStream dis = new DataInputStream(localworker.getInputStream());
                DataOutputStream dos = new DataOutputStream(localworker.getOutputStream());
                for (String s : output)
                    dos.writeUTF(s);
                String msg;
                msg = dis.readUTF();
                String[] input = msg.split(",");
                int otherworker = Integer.parseInt(input[0]);
                int status = Integer.parseInt(input[1]);
                OtherWorkerData[otherworker] = new HashMap<>();
                while (status != -1) {
                    for (i = 2; i < input.length; i += 2) {
                        String key = input[i];
                        Double value = Double.parseDouble(input[i + 1]);
                        OtherWorkerData[otherworker].put(key, value);
                    }
                    System.out.println("Length " + input.length / 2);
                    msg = dis.readUTF();
                    input = msg.split(",");
                    status = Integer.parseInt(input[1]);
                }
                for (i = 2; i < input.length; i += 2) {
                    String key = input[i];
                    Double value = Double.parseDouble(input[i + 1]);
                    OtherWorkerData[otherworker].put(key, value);
                }
                System.out.println("Length " + input.length / 2);
                System.out.println("Receive Data From " + otherworker + " With Length: " + OtherWorkerData[otherworker].size());
                //dos.close();
                //dis.close();
                //localworker.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public HashMap<String, Double> AggregateData() {
        HashMap<String, Double> ret = new HashMap<String, Double>();
        HashMap<String, Double> tmp = new HashMap<String, Double>();
        ret = (HashMap<String, Double>)current.clone();
        int i;
        for (i = 1; i <= WorkerNumber; i++) {
            if (i == WorkerId)
                continue;
            tmp = (HashMap<String, Double>)OtherWorkerData[i].clone();
            for (HashMap.Entry<String, Double> entry : tmp.entrySet()) {
                String key = entry.getKey();
                Double value = entry.getValue();
                if (!ret.containsKey(key)) {
                    ret.put(key, value);
                } else {
                    if (ret.get(key) > 0)
                        ret.put(key, ret.get(key) + value);
                    else
                        ret.put(key, ret.get(key) + value);
                }
            }
        }
        for (String s : Variable) {
            ret.put(s, ret.get(s) + 0.15);
        }
        return ret;
    }


    /* work  main part
    * check CheckPoint0
    * if file doesn't exist (first iteration),
    *      AnswerFistCheck, get StartLine and EndLine
    *      go to Send2Master(Ready2Next)
    * else
    *   read CheckPoint0 file,
    *   check the IterationCount and LocalCount
    *   if equal, go to Send2Master(Ready2Next)
    *   else
    *       Send2Master(Recovery)
    *       if (Recover from CheckPoint)
    *       (crash after Calculation, before send data to others, so no one send data)
    *           read data from CheckPoint0 file
    *           go to Calculation
    *       if (Recover From Send Data)
    *       (crash after send data)
    *           read data from CheckPoint1 file
    *           Master send signal to all worker to redo aggregate data
    *           all worker write CheckPoint1
    *
    * Send2Master(Ready2Next)
    * WaitSignalFromMaster
    * Calculation
    * Write CheckPoint1
    * Send2Master(Ready2Send)
    * WaitSignalFromMaster
    * Send Data to other workers
    * Receive data from other workers
    * Aggregate Data
    * Write CheckPoint0
    * Send2Master(Ready2Next)
    * WaitSignalFromMaster
    * if (MasterACK) goto check CheckPoint0
    * else (Finish) end
    */
    public void work(){
        int step = 0;
        System.out.println("Begin Reading Variable");
        Variable = DataTool.ReadVariable(FilePath);
        System.out.println("Reading Variable Finished");
        while (true) {
            System.out.println("Begin Checking");
            int state = CheckPoint(0, Count);
            int response = -1;
            if (state <= 1) {
                if (state == 0) {
                    // First Iteration
                    System.out.println("First Iteration");
                    System.out.println("Answer First Check");
                    AnswerFirstCheck();
                    System.out.println("Answer First Check Finished");
                }
                System.out.println("Waiting for Next Iteration");
                try{
                    Thread.currentThread().sleep(1000);
                }catch(InterruptedException ie){
                    ie.printStackTrace();
                }
                response = Send2Master(Ready2Next);
                if (response == -1) {
                    System.out.println("Iteration Finished");

                    return;
                } else if (response == 1) {
                    System.out.println("Other Worker Crashed, Redo Data Transmission and Aggregation");

                    System.out.println("Begin Transmission");
                    SendDataToWorkers(current);
                    System.out.println("Transmission Finished");

                    System.out.println("Begin Aggregation");
                    current = AggregateData();
                    System.out.println("Aggregation Finished");

                    System.out.println("Begin Writing CheckPoint0");
                    DataTool.writeVector(Count, WorkerId, StartLine, EndLine, current, CheckPointPath0);
                    System.out.println("Writing CheckPoint0 Finished");

                    continue;
                }
                System.out.println("All Right, Begin Next Iteration");
                Count++;
            } else if (state == 2) {
                // Crashed
                System.out.println("Crashed, Begin Recovery");

                System.out.println("Waiting for Recovery");
                response = Send2Master(Recovery);
                if (response == 2) {
                    //Crash before Ready2Send
                    System.out.println("Crashed Before Transmission, Reload Data From CheckPoint0");
                    HashMap<String, Integer> workerinfo = DataTool.readInfo(CheckPointPath0);
                    Count = workerinfo.get("Count");
                    WorkerId = workerinfo.get("WorkerId");
                    StartLine = workerinfo.get("StartLine");
                    EndLine = workerinfo.get("EndLine");
                    current = DataTool.readVector(CheckPointPath0);
                    System.out.println("Reload From CheckPoint0 Finished");
                } else if (response == 1) {
                    //Crash after Ready2Send, before Ready2Next
                    System.out.println("Crashed During Transmission, Reload Data From CheckPoint1");
                    HashMap<String, Integer> workerinfo = DataTool.readInfo(CheckPointPath1);
                    Count = workerinfo.get("Count");
                    WorkerId = workerinfo.get("WorkerId");
                    StartLine = workerinfo.get("StartLine");
                    EndLine = workerinfo.get("EndLine");
                    current = DataTool.readVector(CheckPointPath1);
                    System.out.println("Reload From CheckPoint1 Finished");
                    SendDataToWorkers(current);
                    current = AggregateData();
                    DataTool.writeVector(Count, WorkerId, StartLine, EndLine, current, CheckPointPath0);
                    continue;
                }
            }
            System.out.println("Begin Calculation");
            compute(StartLine, EndLine);
            System.out.println("Calculation Finished");

            System.out.println("Begin Writing CheckPoint1");
            DataTool.writeVector(Count, WorkerId, StartLine, EndLine, current, CheckPointPath1);
            System.out.println("Writing CheckPoint1 Finished");

            System.out.println("Waiting for Transmission");
            int sendresponse = Send2Master(Ready2Send);
            while (sendresponse != 0) {
                if (sendresponse == -1) break;
                sendresponse = Send2Master(Ready2Send);
            }
            //WaitSignalFromMaster(SendSocket);

            System.out.println("Begin Transmission");
            SendDataToWorkers(current);
            System.out.println("Transmission Finished");

            System.out.println("Begin Aggregation");
            current = AggregateData();
            System.out.println("Aggregation Finished");

            System.out.println("Begin Writing CheckPoint0");
            DataTool.writeVector(Count, WorkerId, StartLine, EndLine, current, CheckPointPath0);
            System.out.println("Writing CheckPoint0 Finished");

            System.out.println("Iteration " + Count + " Finished");
        }

    }


    //一轮计算
    //输入参数为计算起终点,计算结果为更新current
    public void compute(int start, int end){
        System.out.println("start compute\n");
        if(table == null){
            table = DataTool.readOriginFile(FilePath);
            initialVector = DataTool.getInitialVector(table);
            current = initialVector;
        }

        //初始化结果数据，每个数的初始值为0
        HashMap<String, Double> result = (HashMap<String, Double>) initialVector.clone();
        int line = 0;
        for(HashMap.Entry<String, ArrayList<String>> entry: table.entrySet()){
            if(line >= start) {
                if(line < end) {
                    String key = entry.getKey();
                    ArrayList<String> value = entry.getValue();
                    int s = value.size();
                    for (String i: value) {
                        result.put(i, result.get(i) + current.get(key) / s);
                    }
                }else{
                    break;
                }
            }
            line++;
        }

        current = result;
    }

    //主程序
    public static void main(String[] args){
        int i;
        Worker1 worker1 = new Worker1();

        System.out.println("Begin read Configure File");
        worker1.ReadConfigure();
        System.out.println("End read Configure File\n");

        worker1.Count = 0;

        worker1.work();

    }
}
