package br.edu.utfpr.pena.missdc.discovery.evidence.context;

import br.edu.utfpr.pena.missdc.dc.predicates.IndexedPredicate;
import br.edu.utfpr.pena.missdc.dc.predicates.groups.ColumnPredicateGroup;
import br.edu.utfpr.pena.missdc.dc.predicates.groups.NumericalColumnPredicateGroup;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.EqualityIndex;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.LtBinnedIndex;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.LtIndex;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.comparators.EqualityIndexSizeComparator;
import br.edu.utfpr.pena.missdc.dc.predicates.space.PredicateSpace;
import br.edu.utfpr.pena.missdc.discovery.evidence.EvidenceSet;
import br.edu.utfpr.pena.missdc.discovery.evidence.ParallelEvidenceSet;
import br.edu.utfpr.pena.missdc.input.Table;
import br.edu.utfpr.pena.missdc.input.columns.Column;
import br.edu.utfpr.pena.missdc.input.columns.NumericalColumn;
import br.edu.utfpr.pena.missdc.input.sorters.ColumnComparatorByCardinality;
import br.edu.utfpr.pena.missdc.input.sorters.RowComparatorMultiColumn;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ECP implements IEvidenceSetBuilder {

    private static final int BinThreshold = 2000;
    protected int rowNumber;
    protected ArrayList<Integer> rowSequence;
    Map<Integer, BitSet> tidsMissingCols; // tuples has missing value in column related to the predicate in the bitset
    Map<Integer, Set<ColumnPredicateGroup>> skipGroups;
    private final Table table;
    private final PredicateSpace predicateSpace;
    private ParallelEvidenceSet evidenceSet;
    private final RoaringBitmap tableIds;
    private BitSet baseEvidence;
    private List<ColumnPredicateGroup> catPredGroups;
    private List<NumericalColumnPredicateGroup> numPredGroups;

    public ECP(Table table, PredicateSpace predicateSpace) {

        this.table = table;
        this.rowNumber = table.getNUM_RECORDS();
        this.predicateSpace = predicateSpace;

        this.tableIds = new RoaringBitmap();
        this.tableIds.add(0, (long) rowNumber);

        rowSequence = getRowSequence();

    }


    public ECP(Table table, PredicateSpace predicateSpace, boolean sortInput, boolean sortPredicates,
               boolean catPredicatesFirst) {

        this.table = table;
        this.rowNumber = table.getNUM_RECORDS();
        this.predicateSpace = predicateSpace;

        this.tableIds = new RoaringBitmap();
        this.tableIds.add(0, (long) rowNumber);

        rowSequence = getRowSequence();

    }

    public EvidenceSet build() {

        this.evidenceSet = new ParallelEvidenceSet(predicateSpace);

        sortNumericalColumnsMulti(table.getNumericalColumns().values().size());

        buildPredicateIndexes();

        this.baseEvidence = createBaseEvidence();

        EqualityIndexSizeComparator equalityIndexComparator = new EqualityIndexSizeComparator();

        catPredGroups = new ArrayList<>(predicateSpace.getCategoricalPredicateGroups());
        Collections.sort(catPredGroups, equalityIndexComparator);
        Collections.reverse(catPredGroups);

        numPredGroups = new ArrayList<>(predicateSpace.getNumericalPredicateGroups());

        Collections.sort(numPredGroups, equalityIndexComparator);
        Collections.reverse(numPredGroups);

        rowSequence.parallelStream().forEach(tid -> {

            RoaringBitmap tableIdsCopy = tableIds.clone();

            tableIdsCopy.remove(0L, (tid + 1));

            TupleContextHandler eviContext = new TupleContextHandler(tid, tableIdsCopy, baseEvidence);

            for (ColumnPredicateGroup catGroup : catPredGroups) {

                eviContext.updateCategoricalContext(catGroup);

            }

            for (NumericalColumnPredicateGroup numGroup : numPredGroups) {

                eviContext.updateNumericalContext(numGroup);

            }

            eviContext.collectEvidence(evidenceSet, numPredGroups);

        });

        return evidenceSet;
    }

////    public EvidenceSet buildIgnoreNull() {
////
////
////
////        this.evidenceSet = new ParallelEvidenceSet(predicateSpace);
////
////        sortNumericalColumnsMulti(table.getNumericalColumns().values().size());
////
////        buildPredicateIndexes();
////
////
////
////        List<Column> colsWithNulls = table.getColsWithNulls();
////        buildBitsets4Missing(colsWithNulls);
////
////        Map<ColumnPredicateGroup, RoaringBitmap> group2TidsWithNulls = getGroup2TidsWithNulls(colsWithNulls);
////
////
////        this.baseEvidence = createBaseEvidence();
////
////        EqualityIndexSizeComparator equalityIndexComparator = new EqualityIndexSizeComparator();
////
////        catPredGroups = new ArrayList<>(predicateSpace.getCategoricalPredicateGroups());
////        Collections.sort(catPredGroups, equalityIndexComparator);
////        Collections.reverse(catPredGroups);
////
////        numPredGroups = new ArrayList<>(predicateSpace.getNumericalPredicateGroups());
////
////        Collections.sort(numPredGroups, equalityIndexComparator);
////        Collections.reverse(numPredGroups);
////
////        rowSequence.parallelStream().forEach(tid -> {
////
////            RoaringBitmap tableIdsCopy = tableIds.clone();
////
////            tableIdsCopy.remove(0L, (tid + 1));
////
////
////            BitSet reshapedEvidence = (BitSet) baseEvidence.clone();
////
////            BitSet predicatesForColumnWithNull = tidsMissingCols.get(tid);
////            if (predicatesForColumnWithNull != null) {
////                reshapedEvidence.andNot(predicatesForColumnWithNull);
////            }
////
////
////            TupleContextHandler eviContext = new TupleContextHandler(tid, tableIdsCopy, reshapedEvidence);
////
////            Set<ColumnPredicateGroup> group2skip = new HashSet<>();
////            Set<ColumnPredicateGroup> group2skip2 =  skipGroups.get(tid);
////
////            if(group2skip2 != null)
////                group2skip.addAll(group2skip2);
////
////
////            for (ColumnPredicateGroup catGroup : catPredGroups) {
////
////                if(group2skip.contains(catGroup))
////                    continue;
////
////
////                eviContext.updateCategoricalContext(catGroup);
////
////            }
////
////            for (NumericalColumnPredicateGroup numGroup : numPredGroups) {
////
////                if(group2skip.contains(numGroup))
////                    continue;
////
////                eviContext.updateNumericalContext(numGroup);
////
////            }
////
////            //eviContext.collectEvidence(evidenceSet, numPredGroups);
////
////            eviContext.collectEvidenceFixMissing(evidenceSet, numPredGroups, group2TidsWithNulls);
////
////        });
////
////        return evidenceSet;
////    }
////
////
////
////    private void buildBitsets4Missing(List<Column> colsWithNulls) {
////
////        tidsMissingCols = new LinkedHashMap<>();
////
////        skipGroups = new LinkedHashMap<>();
////
////        for (Column col : colsWithNulls) {
////
////            ColumnPredicateGroup group = predicateSpace.getColumn2GroupMap().get(col);
////            BitSet missing = group.getPredicateIDs();
////
////
////
////            for (Integer tid : col.getTIDsMissing()) {
////
////                tidsMissingCols.putIfAbsent(tid, new BitSet());
////                tidsMissingCols.get(tid).or(missing);
////
////                skipGroups.putIfAbsent(tid, new HashSet<>());
////                skipGroups.get(tid).add(group);
////
////            }
////
////
////        }
////    }
//
//
//    private Map<ColumnPredicateGroup, RoaringBitmap> getGroup2TidsWithNulls(List<Column> colsWithNulls) {
//        Map<ColumnPredicateGroup, RoaringBitmap> group2TidsWithNulls = new HashMap<>();
//
//        for (Column col : colsWithNulls) {
//
//                ColumnPredicateGroup group = predicateSpace.getColumn2GroupMap().get(col);
//
//                for (Integer tid : col.getTIDsMissing()) {
//
//                    group2TidsWithNulls.putIfAbsent(group, new RoaringBitmap());
//                    group2TidsWithNulls.get(group).add(tid);
//
//                }
//
//        }
//
//        return group2TidsWithNulls;
//
//    }

    private ArrayList<Integer> getRowSequence() {
        ArrayList<Integer> rowSequence = new ArrayList<>();
        for (int tid = 0; tid < rowNumber; ++tid) {
            rowSequence.add(tid);
        }
        return rowSequence;
    }

    private void buildPredicateIndexes() {

        for (ColumnPredicateGroup catPredicateGroup : predicateSpace.getCategoricalPredicateGroups()) {
            IndexedPredicate eq = catPredicateGroup.getEq();
            EqualityIndex eqIndex = new EqualityIndex(eq);
            eq.setEqualityIndex(eqIndex);
        }

        for (NumericalColumnPredicateGroup numPredicateGroup : predicateSpace.getNumericalPredicateGroups()) {

            IndexedPredicate eq = numPredicateGroup.getEq();
            EqualityIndex eqIndex = new EqualityIndex(eq);
            eq.setEqualityIndex(eqIndex);

            if (eqIndex.getIndex().size() >= BinThreshold) {
                numPredicateGroup.setBinnedLT(true);
                LtBinnedIndex ltBinnedIndex = new LtBinnedIndex(eqIndex, table.getNUM_RECORDS());
                numPredicateGroup.getLt().setLtBinnedIndex(ltBinnedIndex);

            } else {

                LtIndex ltIndex = new LtIndex(eqIndex);
                numPredicateGroup.getLt().setLtIndex(ltIndex);

            }

        }

    }

    private BitSet createBaseEvidence() {

        BitSet baseEvidence = new BitSet();
        // assuming UNEQ in the evidence, then we correct for EQ
        for (ColumnPredicateGroup catGroup : predicateSpace.getCategoricalPredicateGroups()) {
            IndexedPredicate uneq = catGroup.getUneq();
            baseEvidence.set(uneq.getPredicateId());

        }
        for (NumericalColumnPredicateGroup numGroup : predicateSpace.getNumericalPredicateGroups()) {
            // assuming GT in the evidence, then we correct for LT
            IndexedPredicate uneq = numGroup.getUneq();
            IndexedPredicate gt = numGroup.getGt();
            IndexedPredicate gte = numGroup.getGte();
            baseEvidence.set(uneq.getPredicateId());
            baseEvidence.set(gt.getPredicateId());
            baseEvidence.set(gte.getPredicateId());
        }
        return baseEvidence;

    }

    public void sortNumericalColumnsMulti(int levels) {

        ColumnComparatorByCardinality columnComparator = new ColumnComparatorByCardinality();

        List<NumericalColumn> numCols = new ArrayList<>(table.getNumericalColumns().values());

        if (numCols.isEmpty()) {
            return;
        }

        Collections.sort(numCols, columnComparator);
        Collections.reverse(numCols);

        List<Integer> orderedRowIDs = IntStream.range(0, table.getNUM_RECORDS()).boxed().collect(Collectors.toList());

        if (levels > 0 && levels <= numCols.size())
            numCols = numCols.subList(0, levels);

        RowComparatorMultiColumn rowComparator = new RowComparatorMultiColumn(numCols);
        Collections.sort(orderedRowIDs, rowComparator);

        for (Column c : table.getAllColumns().values()) {

            FloatList orderedValues = new FloatArrayList(table.getNUM_RECORDS());

            for (int tid : orderedRowIDs) {
                orderedValues.add(c.getValueAt(tid));

            }

            c.setValuesList(orderedValues);

        }

    }

    public EvidenceSet getEvidenceSet() {
        return evidenceSet;
    }

}