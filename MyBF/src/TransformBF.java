import java.nio.charset.StandardCharsets;
import java.util.BitSet;

public class TransformBF {
    private int seed;
    private int filterSize;
    private int numHashes;
    private BitSet bloomFilter;

    public TransformBF(int setSize, int bitsPerElement){
        this.filterSize = setSize * bitsPerElement;
        this.bloomFilter = new BitSet(filterSize);
        this.numHashes = (setSize == 0) ? 0 : (int) (Math.log(2)*(filterSize/setSize));
        this.seed = 0xd92e493e;
    }

    public int[] murmurHash(byte[] bytes, int seed){
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        int length = bytes.length;

        long h = (seed&0xffffffffl)^(length*m);

        int length8 = length/8;
        int[] hashValues = new int[numHashes];
        for (int i=0 ; i<numHashes; i++){
            for (int j=0; j<length8; j++) {
                final int i8 = j*8;
                long k =  ((long)bytes[i8+0]&0xff)      +(((long)bytes[i8+1]&0xff)<<8)
                        +(((long)bytes[i8+2]&0xff)<<16) +(((long)bytes[i8+3]&0xff)<<24)
                        +(((long)bytes[i8+4]&0xff)<<32) +(((long)bytes[i8+5]&0xff)<<40)
                        +(((long)bytes[i8+6]&0xff)<<48) +(((long)bytes[i8+7]&0xff)<<56);

                k *= m;
                k ^= k >>> r;
                k *= m;

                h ^= k;
                h *= m;
            }

            switch (length%8) {
                case 7: h ^= (long)(bytes[(length&~7)+6]&0xff) << 48;
                case 6: h ^= (long)(bytes[(length&~7)+5]&0xff) << 40;
                case 5: h ^= (long)(bytes[(length&~7)+4]&0xff) << 32;
                case 4: h ^= (long)(bytes[(length&~7)+3]&0xff) << 24;
                case 3: h ^= (long)(bytes[(length&~7)+2]&0xff) << 16;
                case 2: h ^= (long)(bytes[(length&~7)+1]&0xff) << 8;
                case 1: h ^= (long)(bytes[length&~7]&0xff);
                    h *= m;
            };

            h ^= h >>> r;
            h *= m;
            h ^= h >>> r;
            hashValues[i] = Math.abs((int)h)%filterSize;
        }
        return hashValues;
    }

    public void add(String s){
        if (s != null){
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            for(int index : murmurHash(b,seed)){
                bloomFilter.set(index);
            }
        }
    }

    public boolean appears(String s){
        if (s == null) {
            return false;
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        for (int index : murmurHash(b,seed)) {
            if (!bloomFilter.get(index)) {
                return false;
            }
        }
        return true;
    }

    public int filterSize(){
        return filterSize;
    }

    public int dataSize(){
        return bloomFilter.cardinality();
    }

    public int numHashes(){
        return numHashes;
    }
}