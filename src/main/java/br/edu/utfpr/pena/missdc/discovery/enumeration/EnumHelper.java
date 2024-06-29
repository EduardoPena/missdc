package br.edu.utfpr.pena.missdc.discovery.enumeration;


import br.edu.utfpr.pena.missdc.utils.bitset.IBitSet;
import br.edu.utfpr.pena.missdc.utils.bitset.LongBitSet;
import br.edu.utfpr.pena.missdc.utils.search.TreeSearch;

import java.util.*;

public class EnumHelper {

    public static List<BitSet> minimizeEviset(Set<BitSet> eviset) {

        ArrayList<IBitSet> sortedNegCover = new ArrayList<>();
        eviset.forEach(evi -> sortedNegCover.add(LongBitSet.FACTORY.create(evi)));

        sortedNegCover.sort(new Comparator<IBitSet>() {
            @Override
            public int compare(IBitSet o1, IBitSet o2) {
                int erg = Integer.compare(o2.cardinality(), o1.cardinality());
                return erg != 0 ? erg : o2.compareTo(o1);
            }
        });

        TreeSearch neg = new TreeSearch();
        sortedNegCover.forEach(invalid -> addInvalidToNeg(neg, invalid));

        final ArrayList<BitSet> list = new ArrayList<>();

        neg.forEach(invalidFD -> list.add(invalidFD.toBitSet()));

        return list;
    }

    private static void addInvalidToNeg(TreeSearch neg, IBitSet invalid) {
        if (neg.findSuperSet(invalid) != null)
            return;

        neg.getAndRemoveGeneralizations(invalid);
        neg.add(invalid);
    }
}