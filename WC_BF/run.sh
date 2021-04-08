cd /home/xidian/class

hadoop com.sun.tools.javac.Main ./WC_BF/WordCount_BF.java ./BF_StrongShuffle/Client.java

jar cf BF_StrongShuffle_wc.jar ./WC_BF/WordCount_BF*.class ./WC_BF/Client*.class

