# macpepdb_peptide_hydrophobicity

Peptide hydrophobicity prediction, part of the
[`macpepdb`](https://github.com/medbioinf/macpepdb) ecosystem.

Implements the Krokhin SSRCalc model:

> O.V. Krokhin, R. Craig, V. Spicer, W. Ens, K.G. Standing, R.C. Beavis, J.A. Wilkins,
> *An Improved Model for Prediction of Retention Times of Tryptic Peptides in Ion Pair
> Reversed-phase HPLC: Its Application to Protein Peptide Mapping by Off-Line HPLC-MALDI MS*,
> Molecular & Cellular Proteomics.

Reimplemented from the [Thermo Fisher Peptide Analyzer](https://downloads.thermofisher.com/assets/apps/peptide-analyzer/ssrcalc3.js),
itself a chain of reimplementations from Perl → C → Java → C# → JavaScript going back to 2006.
