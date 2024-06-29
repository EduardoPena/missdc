package br.edu.utfpr.pena.missdc.dc.predicates.indexes.comparators;


import br.edu.utfpr.pena.missdc.dc.predicates.groups.ColumnPredicateGroup;
import java.util.Comparator;

public class CardinalityBasedColumnPredicateGroupComparator implements Comparator<ColumnPredicateGroup> {

	@Override
	public int compare(ColumnPredicateGroup g1, ColumnPredicateGroup g2) {
		return Long.compare(g1.getEq().getPredicate().getCol1().getCardinality(),
				g2.getEq().getPredicate().getCol1().getCardinality());
	}

}