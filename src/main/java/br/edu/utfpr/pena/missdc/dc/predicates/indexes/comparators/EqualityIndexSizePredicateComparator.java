package br.edu.utfpr.pena.missdc.dc.predicates.indexes.comparators;

import br.edu.utfpr.pena.missdc.dc.predicates.IndexedPredicate;

import java.util.Comparator;

public class EqualityIndexSizePredicateComparator implements Comparator<IndexedPredicate> {

	@Override
	public int compare(IndexedPredicate p1, IndexedPredicate p2) {
		return Integer.compare(p1.getEqualityIndex().getIndex().size(), p2.getEqualityIndex().getIndex().size());
	}

}