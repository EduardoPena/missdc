package br.edu.utfpr.pena.missdc;

import br.edu.utfpr.pena.missdc.imputation.MissDC;

public class MissDCMocker {

    public static void main(String[] args) throws Exception {
        String dirtyFile =  "data/flights/dirty.csv"; // args[0];
        MissDC imputer = new MissDC(dirtyFile);
        imputer.run();
    }
}