package br.edu.utfpr.pena.missdc.discovery;

import br.edu.utfpr.pena.missdc.dc.predicates.space.PredicateSpace;
import br.edu.utfpr.pena.missdc.dc.predicates.space.builder.PredicateSpaceBuilder;
import br.edu.utfpr.pena.missdc.discovery.enumeration.DCEnumeration;
import br.edu.utfpr.pena.missdc.discovery.enumeration.HybridEvidenceInversion;
import br.edu.utfpr.pena.missdc.discovery.enumeration.adc.ADC;
import br.edu.utfpr.pena.missdc.discovery.evidence.EvidenceSet;
import br.edu.utfpr.pena.missdc.discovery.evidence.context.IEvidenceSetBuilder;
import br.edu.utfpr.pena.missdc.discovery.evidence.context.RowECP;
import br.edu.utfpr.pena.missdc.input.Table;
import br.edu.utfpr.pena.missdc.input.reader.CSVInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Set;

public class DCDiscoverer {

    private static final Logger log = LoggerFactory.getLogger(DCDiscoverer.class);
    private static double approxThreshold = 0.0001d;
    private final boolean useApproximateDCs;
    private EvidenceSet evidenceSet;
    private CSVInput input;

    public DCDiscoverer(String dataset, boolean useApproximateDCs) throws Exception {
        this.input = new CSVInput(dataset);
        this.useApproximateDCs = useApproximateDCs;
    }

    public DCDiscoverer(String dataset) throws Exception {
        this(dataset, false);
    }

    public EvidenceSet getEvidenceSet() {
        return evidenceSet;
    }

    public Set<BitSet> run() throws Exception {

        log.info("DC Discovery on " + input.getDataFileName() + " ...");
        Table table = input.getTable();
        PredicateSpace predicateSpace = new PredicateSpaceBuilder().build(table);

        log.info("Building evidence set...");
        IEvidenceSetBuilder evidenceSetBuilder = new RowECP(table, predicateSpace);
        EvidenceSet evidenceSet = evidenceSetBuilder.build();

        if (useApproximateDCs) {
            log.info("Searching for approximate DCs...");
            DCEnumeration dcEnumeration = new ADC(predicateSpace, evidenceSet, approxThreshold);
            Set<BitSet> dcs = dcEnumeration.searchDCs();
            log.info(dcs.size() + " DCs found");
            return dcs;
        }else{
            log.info("Searching for DCs...");
            DCEnumeration dcEnumeration = new HybridEvidenceInversion(predicateSpace, evidenceSet);
            Set<BitSet> dcs = dcEnumeration.searchDCs();
            log.info(dcs.size() + " DCs found");
            return dcs;
        }

    }

}