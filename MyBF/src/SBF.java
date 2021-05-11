import java.util.ArrayList;

public class SBF {
    public static int distinct = 0;
    private final int INITIAL_SIZE = 1000;
    private static int totalSize = 0;
    private static int setSize;
    private static int bitsPerElement;
    private static int elementsCount;
    private static int filterCount;

    private ArrayList<TransformBF> allFilters= new ArrayList<>();

    public SBF(int bitsPerE){
        bitsPerElement = bitsPerE;
        elementsCount = 0;
        filterCount = 0;
        setSize = INITIAL_SIZE;
        this.allFilters.add(new TransformBF(setSize,bitsPerElement));
    }

    public void add(String s){
        if (s != null) {
            if(elementsCount >= setSize){
                totalSize += setSize * bitsPerElement;
                filterCount++;
                elementsCount=0;
                setSize *= 2;
                allFilters.add(filterCount,new TransformBF(setSize,bitsPerElement));
            }

            if(!appears(s)){
                distinct ++;
                elementsCount++;
                allFilters.get(filterCount).add(s);
            }


        }
    }

    public boolean appears(String s){
        if (s == null) {
            return false;
        }
        for (TransformBF filter : allFilters)
        {
            if (filter.appears(s))
                return true;
        }
        return false;
    }

    public int filterSize(){
        return allFilters.get(filterCount).filterSize();
    }

    public int dataSize(){
        return 	allFilters.get(filterCount).dataSize();
    }

    public int numHashes(){
        return allFilters.get(filterCount).numHashes();
    }

    public double memSize(){ return totalSize/8192/1024; }
}