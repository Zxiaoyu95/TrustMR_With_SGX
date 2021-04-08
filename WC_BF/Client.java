

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.*;
import java.io.*;
public class Client {
	
    static int max;

    public static int startClient(ArrayList<String> strArr) {
        if(strArr == null){
             return max;
        }
        try {
            Socket s = new Socket("172.18.12.10",8888);
            //构建IO
            InputStream is = s.getInputStream();
            OutputStream os = s.getOutputStream();
            //BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
            ObjectOutputStream out = new ObjectOutputStream(os);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            
//            Thread readThread = new Thread(){
//                public void run(){
//                    while(true){
//                        String msg = null;
//                        try {
//                            msg = br.readLine();
//                            while(msg == null){
//                            	System.out.print("wait.....");
//                            }
//                            max = Integer.parseInt(msg);
//                           
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                        System.out.println(msg+"#"+max);
//                        
//                    }
//                }
//            };
            Thread writeThread = new Thread(){
                public void run(){
                    //while(true){
                        try {
                            out.writeObject(strArr);
                            out.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    //}
                }
            };
            //readThread.start();
            writeThread.start();  
            String msg = null;
            while(true){
            	try {
                    msg = br.readLine();
                    if(msg != null){
                    	max = Integer.parseInt(msg);
                    	break;
                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
            	System.out.println(msg+"#"+max);
            }
            
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return max;
    }
}
