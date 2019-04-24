import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class Apriori {
    private List<String> basketlist = new ArrayList<String>();
    private int basketlen;
    private Set<String> SingleItemFreqSet ;
    private double thresold;
    private int TopK;
    private HashSet<HashSet<String>> CandidatePairSet = new HashSet<HashSet<String>>();
    private HashSet<HashSet<String>> CandidateTripleSet = new HashSet<HashSet<String>>();
    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
    private void setSingleFrequentItemSet(){
        Map<String,Integer> freqdict = new HashMap<String, Integer>();
        for (String line:basketlist){
            HashSet<String> words = new HashSet<String>(Arrays.asList(line.split(" ")));
            for(String word:words){
                if(freqdict.get(word)==null){
                    freqdict.put(word,1);
                }
                else{
                    freqdict.put(word,freqdict.get(word)+1);
                }
            }
        }
        for(Iterator<Map.Entry<String, Integer>> it = freqdict.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Integer> entry = it.next();
            if((float)entry.getValue()/(float)basketlen<thresold) {
                it.remove();
            }
        }
        Map<String,Integer> sortedMap = sortByValue(freqdict);
        SingleItemFreqSet = new HashSet<String>();
        Iterator<String> it = sortedMap.keySet().iterator();
        if(sortedMap.size()>TopK){
                int count=0;
                while(count<TopK && it.hasNext()) {
                    SingleItemFreqSet.add(it.next());
                    count++;
                }
        }
        else{
            while(it.hasNext()){
                SingleItemFreqSet.add(it.next());
            }
        }
    }
    private void setCandidatePair(){
        List<String> SingleItemFreqlist = new ArrayList<String>(SingleItemFreqSet);

        for(int i=0;i<SingleItemFreqlist.size();i++){
            for(int j=i+1;j<SingleItemFreqlist.size();j++){
                if(SingleItemFreqlist.get(i).equals(SingleItemFreqlist.get(j)))continue;
                HashSet<String> l = new HashSet<String>();
                l.add(SingleItemFreqlist.get(i));
                l.add(SingleItemFreqlist.get(j));
                CandidatePairSet.add(l);
            }
        }
    }
    private void setCandidateTriple(){
        Map<HashSet<String>,Integer> truefreqpairdict = GetTrulyPairFreqDict();
        HashSet<HashSet<String>> TopKpairSet = new HashSet<HashSet<String>>();
        HashSet<String> trulyfreqitemfrompair = new HashSet<String>();
        Iterator<HashSet<String>> it = truefreqpairdict.keySet().iterator();
        int count=0;
        while (it.hasNext()) {
            if(count==TopK)break;
            TopKpairSet.add(it.next());
            count++;
        }
        for(HashSet<String> p:TopKpairSet){
               trulyfreqitemfrompair.addAll(p);
        }
        List<String> candidatetriple  =  new ArrayList<String>(trulyfreqitemfrompair);
        for(int i=0;i<candidatetriple.size();i++){
            for(int j=i+1;j<candidatetriple.size();j++){
                for(int k =j+1;k<candidatetriple.size();k++) {
                    HashSet<String> l = new HashSet<String>();
                    l.add(candidatetriple.get(i));
                    l.add(candidatetriple.get(j));
                    l.add(candidatetriple.get(k));
                    CandidateTripleSet.add(l);
                }
            }
        }
    }
    private Boolean issubset(HashSet<String> tuples, HashSet<String> items){
        for(String i  :tuples){
            if(!items.contains(i))return false;
        }
        return true;
    }
    private Map<HashSet<String>,Integer> GetTrulyPairFreqDict(){
        setCandidatePair();
        Map<HashSet<String>,Integer> freqdict = new HashMap<HashSet<String>, Integer>();
        int sum=0;
        for(String line : basketlist){
            sum++;
            System.out.println((float)sum/(float)basketlen);
            HashSet<String> items = new HashSet<String>(Arrays.asList(line.split(" ")));
            for(HashSet<String> pair : CandidatePairSet) {
                if (issubset(pair, items)) {
                    if (freqdict.get(pair) == null) {
                        freqdict.put(pair, 1);
                    } else {
                        freqdict.put(pair, freqdict.get(pair) + 1);
                    }
                }
            }
        }
        for(Iterator<Map.Entry<HashSet<String>, Integer>> it = freqdict.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<HashSet<String>, Integer> entry = it.next();
            if((float)entry.getValue()/(float)basketlen<thresold) {
                it.remove();
            }
        }
        return sortByValue(freqdict);
    }
    public Map<HashSet<String>,Integer> GetTrulyTripleFreqDict(){
        setCandidateTriple();
        Map<HashSet<String>,Integer> freqdict = new HashMap<HashSet<String>, Integer>();
        int sum=0;
        for(String line : basketlist){
            sum++;
            System.out.println((float)sum/(float)basketlen);
            HashSet<String> items = new HashSet<String>(Arrays.asList(line.split(" ")));
            for(HashSet<String> tirple : CandidateTripleSet) {
                if (issubset(tirple, items)) {
                    if (freqdict.get(tirple) == null) {
                        freqdict.put(tirple, 1);
                    } else {
                        freqdict.put(tirple, freqdict.get(tirple) + 1);
                    }
                }
            }
        }
        for(Iterator<Map.Entry<HashSet<String>, Integer>> it = freqdict.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<HashSet<String>, Integer> entry = it.next();
            if((float)entry.getValue()/(float)basketlen<thresold) {
                it.remove();
            }
        }
        return sortByValue(freqdict);
    }
    public void setThresold(double t){
        this.thresold = t;
    }
    public void setTopK(int k){this.TopK = k;}
    private void printTopKpair(){
        Map<HashSet<String>,Integer> truefreqdict = GetTrulyPairFreqDict();
        Iterator<Map.Entry<HashSet<String>, Integer>> it = truefreqdict.entrySet().iterator();
        int count=0;
        while (it.hasNext()) {
            if(count==TopK)break;
            Map.Entry<HashSet<String>, Integer> pair = it.next();
            System.out.println(pair.getKey()+" "+pair.getValue());
            count++;
        }
       // System.out.println("hello");
    }
    private void printTopKTriple(){
           Map<HashSet<String>,Integer> truefreqdict = GetTrulyTripleFreqDict();
           Iterator<Map.Entry<HashSet<String>, Integer>> it = truefreqdict.entrySet().iterator();
           int count=0;
           while (it.hasNext()) {
               if(count==TopK)break;
               Map.Entry<HashSet<String>, Integer> pair = it.next();
               System.out.println(pair.getKey()+" "+pair.getValue());
               count++;
        }
    }
    private Apriori(List<File> flist,int TopK,double thresold){
        this.TopK = TopK;
        this.thresold = thresold;
        try {
            for(File f :flist) {
                BufferedReader br = new BufferedReader(new FileReader(f.getAbsolutePath()));
                String line = br.readLine();
                while (line != null) {
                    this.basketlist.add(line);
                    line = br.readLine();
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        this.basketlen = basketlist.size();
        setSingleFrequentItemSet();
    }
    public Apriori(String input,int TopK,double thresold){
        this.basketlist = new ArrayList<String>(Arrays.asList(input.split("\n")));
        setSingleFrequentItemSet();
    }
    public static void main(String[] args) throws Exception {
          int TopK=40;
          double thresold = 0.005;
          System.out.println("Counting frequent pairs in dataset");
          long start = System.currentTimeMillis();
          List<File> flist = new ArrayList<File>();
          flist.add(new File("shakespeare-basket1"));
          flist.add(new File("shakespeare_basket2"));
          Apriori a = new Apriori(flist,TopK,thresold);
          a.printTopKTriple();
          long end = System.currentTimeMillis();
          long duration = (end-start)/1000;
          System.out.println("Total time: "+duration);
    }

}
