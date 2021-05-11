import java.nio.charset.StandardCharsets;

import java.util.BitSet;



public class MyBloomFilter {

    private int seed;

    private int filterSize;

    private int numHashes;

    private BitSet bloomFilter;



    public MyBloomFilter(double wrongProb, int bitsPerSplit){

        this.numHashes = (int)Math.ceil(-(Math.log(wrongProb)/Math.log(2)));

        this.filterSize = bitsPerSplit * numHashes;

        this.bloomFilter = new BitSet(filterSize);

        this.seed = 0xd93e493e;

    }



    public int[] murmurHash(byte[] bytes,int seed,int numhash,int fsize){

        final long m = 0xc6a4a7935bd1e995L;

        final int r = 47;

        int length = bytes.length;



        long h = (seed&0xffffffffl)^(length*m);



        int length8 = length/8;

        int[] hashValues = new int[numhash];

        for (int i=0 ; i<numhash; i++){

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

            hashValues[i] = Math.abs((int)h)%fsize;

        }

        return hashValues;

    }





    public void add(String s,int numhash,int fsize){

        if (s != null ) {

            byte[] b = s.getBytes(StandardCharsets.UTF_8);

            for(int index : murmurHash(b,seed,numhash,fsize)){

                bloomFilter.set(index,true);

            }

        }

    }



    public boolean appears(String s,int numhash, int fsize){

        if (s == null) {

            return false;

        }

        byte[] b = s.getBytes(StandardCharsets.UTF_8);

        for (int index : murmurHash(b,seed,numhash,fsize)) {

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
