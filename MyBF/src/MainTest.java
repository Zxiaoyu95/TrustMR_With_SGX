public class MainTest {
    public static void main(String[] args){

        double wrongP = 0.1;
        int bitsPerHash = 8192;
        int setSize = 1000000;
        DynamicFilterGroup BFR = new DynamicFilterGroup(wrongP,bitsPerHash);

        long startTime = System.currentTimeMillis();
        for(int i =0;i<setSize;i++){
            BFR.add("a"+i);
        }
        System.out.println("GDBF不重复的个数——"+BFR.count);
        System.out.println("分组个数--"+BFR.groupNum());
        BFR.showHashNumArr(wrongP,bitsPerHash,setSize,BFR.count);
        long endTime = System.currentTimeMillis();
        System.out.println("GDBF程序运行时间：" + (endTime - startTime) + "ms");
        System.out.println("-------------------------------------");
//        long startTime1 = System.currentTimeMillis();
//        SBF sbf = new SBF(128);
//        for(int i =0;i<setSize;i++){
//            sbf.add("a"+i);
//        }
//        long endTime1 = System.currentTimeMillis();
//        int pra = setSize-SBF.distinct;
//        double mem = sbf.memSize();
//        System.out.println("SBF不重复的个数——"+SBF.distinct);
//        System.out.println("SBF误判率为——"+pra/setSize);
//        System.out.println("SBF占用空间为——"+ mem + "mb");
//        System.out.println("SBF程序运行时间：" + (endTime1 - startTime1) + "ms");
    }
}
