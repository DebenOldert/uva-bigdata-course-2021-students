package nl.uva.bigdata.hadoop.assignment2;


import java.util.*;

class Record<K  extends Comparable<K>, V> {
    K key;
    V value;

    public Record(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}

interface MapFunction<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2> {
    Collection<Record<K2, V2>> map(Record<K1, V1> inputRecord);
}

interface ReduceFunction<K2 extends Comparable<K2>, V2, V3> {
    Collection<Record<K2, V3>> reduce(K2 key, Collection<V2> valueGroup);
}

public class MapReduceEngine<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2, V3> {

    private Collection<Record<K2, V2>> runMapPhase(
            Collection<Record<K1, V1>> inputRecords,
            MapFunction<K1, V1, K2, V2> map
    ) {
        //READ DATA

        Collection<Record<K2, V2>> result = null;

        for(Record<K1, V1> r: inputRecords){
            System.out.println(r.key + " => " + r.value);
            if(result == null){
                result = map.map(r);
            }
            else{
                result.addAll(map.map(r));
            }
        }
        System.out.println("===========");

        return result;
    }

    private Collection<Collection<Record<K2, V2>>> partitionMapOutputs(
            Collection<Record<K2, V2>> mapOutputs,
            int numPartitions) {
        //Split collection

        int size = mapOutputs.size();

        ArrayList<Collection<Record<K2, V2>>> parts = new ArrayList<>();

//        for(int offset=0; offset<numPartitions; offset++){
//            parts.set(offset, new ArrayList<Record<K2, V2>>());
//            for(int i=offset; i<size; i+=numPartitions){
//                parts.get(offset).add(mapOutputs)
//            }
//        }

        int offset = 0;
        for(Record<K2, V2> r: mapOutputs){
            System.out.println(r.key + " => " + r.value);
            if(offset >= numPartitions){
                offset = 0;
            }
            if(offset >= parts.size()){
                parts.add(new ArrayList<Record<K2, V2>>());
            }
            parts.get(offset).add(r);
            offset++;
        }

        System.out.println("Parts: "+numPartitions);
        System.out.println("===========");

        return parts;

        //throw new IllegalStateException("Not implemented");
    }

    private Map<K2, Collection<V2>> groupReducerInputPartition(Collection<Record<K2, V2>> reducerInputPartition) {

        //Group keys

        Map<K2, Collection<V2>> groups = new HashMap<>();

        for(Record<K2, V2> r: reducerInputPartition){
            if(!groups.containsKey(r.key)){
                groups.put(r.key, new ArrayList<V2>());
            }
            groups.get(r.key).add(r.value);
        }

        return groups;

        //throw new IllegalStateException("Not implemented");
    }

    private Collection<Record<K2, V3>> runReducePhaseOnPartition(
            Map<K2, Collection<V2>> reducerInputs,
            ReduceFunction<K2, V2, V3> reduce
    ) {
        ArrayList<Record<K2, V3>> results = new ArrayList<>();

        for(Map.Entry<K2, Collection<V2>> e: reducerInputs.entrySet()){
            results.addAll(reduce.reduce(e.getKey(), e.getValue()));
        }

        for(Record<K2, V3> r: results){
            System.out.println(r.key + " => " + r.value);
        }

        return results;

        //throw new IllegalStateException("Not implemented");
    }

    public Collection<Record<K2, V3>> compute(
        Collection<Record<K1, V1>> inputRecords,
        MapFunction<K1, V1, K2, V2> map,
        ReduceFunction<K2, V2, V3> reduce,
        int numPartitionsDuringShuffle
    ) {

        Collection<Record<K2, V2>> mapOutputs = runMapPhase(inputRecords, map);
        Collection<Collection<Record<K2, V2>>> partitionedMapOutput =
                partitionMapOutputs(mapOutputs, numPartitionsDuringShuffle);

        assert numPartitionsDuringShuffle == partitionedMapOutput.size();

        List<Record<K2, V3>> outputs = new ArrayList<>();

        for (Collection<Record<K2, V2>> partition : partitionedMapOutput) {
            Map<K2, Collection<V2>> reducerInputs = groupReducerInputPartition(partition);

            outputs.addAll(runReducePhaseOnPartition(reducerInputs, reduce));
        }


        return outputs;
    }

}
