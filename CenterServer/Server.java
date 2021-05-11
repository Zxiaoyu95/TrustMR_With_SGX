import java.io.BufferedReader;

import java.io.BufferedWriter;

import java.io.IOException;

import java.io.InputStreamReader;

import java.io.OutputStreamWriter;

import java.net.ServerSocket;

import java.net.Socket;

import java.util.Random;

import java.util.*;

import java.io.*;

public class Server {

    static int max = Integer.MIN_VALUE;

    static int mapper_num = 14;

    static int number =0;
    static double wrongP = 0.000001;

    static int bitsPerHash = 8192*5;
    static DynamicFilterGroup dfg = new DynamicFilterGroup(wrongP,bitsPerHash);


    public static void main(String[] args)throws ClassNotFoundException, 
        InstantiationException, IllegalAccessException{

        ServerSocket ss = null;      

        try {

            ss = new ServerSocket(8888);

            System.out.println("启动服务器>>>>>>");

        } catch (IOException e1) {

            // TODO Auto-generated catch block

            e1.printStackTrace();

        }

        while(true){
            
 //           if(number == mapper_num){
                 
   //              break;
     //       }
            number++;

            try {

                Socket s = ss.accept();
                
                System.out.println("Mapper:"+number+"已连接到服务器");
                 
                Thread readThread = new Thread(){

                    public void run(){
                            try {
                               ObjectInputStream oi = new ObjectInputStream(s.getInputStream()); 
                                         
                                
                                ArrayList<String> arr = (ArrayList<String>)oi.readObject();
                                for(String str : arr){
                             
	                                System.out.println("get the key:"+str);
                                        synchronized(dfg){
                                            dfg.add(str);
                                        }                                   
                                }
                             

                            } catch (IOException e) {

                                e.printStackTrace();

                            } catch(ClassNotFoundException e){
                             
                            }

                    }

                };

                Thread writeThread = new Thread(){

                    public void run(){
                       if(number >= mapper_num){ 
                        try {
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream())); 
                            max = dfg.count;
                            if(number >= mapper_num){
                           	 System.out.println("distinct key number:"+max);
                            }
                            int random = new Random().nextInt(50);
                            bw.write(max +"\n");
                            bw.flush();
                         } catch (IOException e) {

                                e.printStackTrace();

                            }    
                        }                    

                    }

                };

                readThread.start();
                Thread.sleep(1000);
                writeThread.start();
            
            } catch (IOException e) {

                e.printStackTrace();

            } catch (InterruptedException e) {

                e.printStackTrace();

            }

        }

    }

}
