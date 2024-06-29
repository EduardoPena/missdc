package br.edu.utfpr.pena.missdc.utils.search.helpers;


import br.edu.utfpr.pena.missdc.utils.bitset.IBitSet;
import br.edu.utfpr.pena.missdc.utils.bitset.LongBitSet;

import java.util.*;

public class IndexProvider<T> {
    private final Map<T, Integer> indexes = new HashMap<>();
    private final List<T> objects = new ArrayList<>();

    private int nextIndex = 0;

    public static <A extends Comparable<A>> IndexProvider<A> getSorted(IndexProvider<A> r) {
        IndexProvider<A> sorted = new IndexProvider<>();
        List<A> listC = new ArrayList<A>(r.objects);
        Collections.sort(listC);
        for (A c : listC) {
            sorted.getIndex(c);
        }
        return sorted;
    }

    public Integer getIndex(T object) {
        Integer index = indexes.putIfAbsent(object, Integer.valueOf(nextIndex));
        if (index == null) {
            index = Integer.valueOf(nextIndex);
            ++nextIndex;
            objects.add(object);
        }
        return index;
    }

    public T getObject(int index) {
        return objects.get(index);
    }

    public IBitSet getBitSet(Iterable<T> objects) {
        IBitSet result = LongBitSet.FACTORY.create();
        for (T i : objects) {
            result.set(getIndex(i).intValue());
        }
        return result;
    }

    public Collection<T> getObjects(IBitSet bitset) {
        ArrayList<T> objects = new ArrayList<>();
        for (int i = bitset.nextSetBit(0); i >= 0; i = bitset.nextSetBit(i + 1)) {
            objects.add(getObject(i));
        }
        return objects;
    }

    public int size() {
        return nextIndex;
    }
}