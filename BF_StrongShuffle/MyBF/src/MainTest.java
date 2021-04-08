import java.io.*;
public class MainTest {
    public static void main(String[] args){
        long startTime = System.currentTimeMillis();
        double wrongP = 0.000001;
        int bitsPerHash = 8192*5;
        int setSize = 10000;
        DynamicFilterGroup BFR = new DynamicFilterGroup(wrongP,bitsPerHash);
        try{
    		File file = new File("/home/xidian/class/WCTest.txt");
    		if(file.isFile() && file.exists()) {
      			InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "utf-8");
      			BufferedReader br = new BufferedReader(isr);
      			String lineTxt = null;
      			while ((lineTxt = br.readLine()) != null) {
                        	String[] arr = lineTxt.split(" ");
                        	for(String str : arr){
                            		BFR.add(str);
				}
                        }
      			br.close();
    		} 
		else {
      		System.out.println("文件不存在!");
    		}
  	    } catch (Exception e) {
   		 System.out.println("文件读取错误!");
  		}
        //for(int i =0;i<setSize;i++){
        //    BFR.add("a"+i);
       // }
        System.out.println("不重复的个数——"+BFR.count);
        System.out.println("分组个数--"+BFR.groupNum());
        //BFR.showHashNumArr(wrongP,bitsPerHash,setSize,BFR.count);
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");

    }
}
