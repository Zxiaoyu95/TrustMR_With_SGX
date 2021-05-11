import java.text.DecimalFormat;

import java.util.ArrayList;

import java.util.HashMap;

import java.util.LinkedHashMap;



public class DynamicFilterGroup {

    public static int count = 0;

    private final static int maxFilterCount = 30;

    private final static double r = 0.5;//误判率缩小倍数

    private final int s = 1;//每一分片位数扩充倍数

    private int bitsPerSplit;//m

    private int elementsCount;//统计当前布隆过滤器元素个数

    private int filterCount;//组内过滤器下标（个数）

    private double wrongProb;//组内每个过滤器的误判率

    private int Gid;//分组下标（个数）

    public static HashMap<Integer,Integer> groupFilterSize = new LinkedHashMap<>();//存放每个分组中哈希函数个数

    public static HashMap<Integer,Integer> groupFilterNum = new LinkedHashMap<>();

    private static ArrayList<ArrayList<MyBloomFilter>> eDBF;

    private  ArrayList<MyBloomFilter> allFilters;

    public DynamicFilterGroup(double wrongProb, int bitsPerSplit){//初始化第一个布隆过滤器参数

        this.bitsPerSplit = bitsPerSplit;

        this.elementsCount = 0;

        this.filterCount = 0;

        this.Gid = 0;

        this.wrongProb = wrongProb;

        allFilters = new ArrayList<>();

        allFilters.add(filterCount,new MyBloomFilter(wrongProb,bitsPerSplit));

        eDBF = new ArrayList<>();

        this.eDBF.add(Gid,allFilters);

        groupFilterSize.put(Gid,eDBF.get(Gid).get(0).numHashes());

        groupFilterNum.put(Gid,1);

    }



    public void add(String str){

        if (str != null) {

            if(filterCount < maxFilterCount){//在同一组内

                if(elementsCount >= bitsPerSplit*Math.log(2)){//组内过滤器元素数量溢出

                    filterCount++;

                    groupFilterNum.put(Gid,groupFilterNum.getOrDefault(Gid,1)+1);

                    elementsCount = 0;

                    allFilters.add(filterCount,new MyBloomFilter(wrongProb,bitsPerSplit));

                }

                else{//继续插入

                    if(!appears(str)){

                        elementsCount++;

                        count ++;

                        int t = groupFilterSize.get(Gid);

                        int size = t*bitsPerSplit;

                        eDBF.get(Gid).get(filterCount).add(str,t,size);

                    }

                }

            }

            else{//设置新的组参数

                filterCount = 0;

                Gid++;//组号++

                bitsPerSplit *= s;//容量指数型增加

                double tmp1 = 1 - Math.pow(r,Gid)*(1-(double)Math.pow(1-wrongProb,maxFilterCount));

                double tmp2 = Math.pow(tmp1,1d/maxFilterCount);

                double wrongProbNew = 1 - tmp2;//更新误判率

                if(wrongProbNew == 0){

                    System.out.println();

                }

                elementsCount = 0;

                allFilters = new ArrayList<>();

                allFilters.add(filterCount,new MyBloomFilter(wrongProbNew,bitsPerSplit));

                eDBF.add(Gid,allFilters);

                groupFilterSize.put(Gid,eDBF.get(Gid).get(0).numHashes());

                add(str);

            }

        }

    }



    public boolean appears(String str){

        if (str == null) {

            return false;

        }

        int gid = 0;

        for (ArrayList<MyBloomFilter> G : eDBF)

        {

            int tmpcount = groupFilterSize.get(gid);

            int tmp = tmpcount*bitsPerSplit;

           for(MyBloomFilter filter : G){

               if (filter.appears(str,tmpcount,tmp)){

                   return true;

               }

           }

           gid++;

        }

        return false;

    }





    public int filterSize(){

        return eDBF.get(Gid).get(filterCount).filterSize();

    }



    public int dataSize(){

        return 	eDBF.get(Gid).get(filterCount).dataSize();

    }



    public int numHashes(){

        return eDBF.get(Gid).get(filterCount).numHashes();

    }



    public int groupNum(){

        return Gid+1;

    }

    public static void showHashNumArr(double wrongP,int bitsPerHash,int setSize,int correctNum){

        DecimalFormat df = new DecimalFormat("0.0000000000");

        float count = 0;

        for(Integer id : groupFilterSize.keySet()){

            int nh = groupFilterSize.get(id);//组内哈希函数个数

            int mperG = nh * bitsPerHash;//组内每个过滤器占用的位数

            count += (mperG/8192)*groupFilterNum.get(id);

            System.out.println(id+"组哈希函数个数为"+nh);

            System.out.println(id+"组每个过滤器占用位数为"+mperG+"byte="+mperG/8192+"kb");

            System.out.println(id+"组占用空间为"+(mperG/8192)*groupFilterNum.get(id)+"kb");

            System.out.println("***********************************");

        }

        System.out.println("总共占用的内存大小为"+count+"kb="+count/1024+"mb");

        double resP = 1-((double)correctNum/setSize);

        System.out.println("整体实际误判率为"+df.format(resP));

        double pg0 = 1-(double)Math.pow((1-wrongP),maxFilterCount);

        double Pc = 1/(1-r)*pg0;

        System.out.println("整体理论误判率为"+df.format(Pc));

    }

}
