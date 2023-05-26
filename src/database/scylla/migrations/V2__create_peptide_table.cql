CREATE TABLE macpep.peptides (                                                                                                     [15/63]
    partition smallint,                                                                                                                   
    mass bigint,                                                                                                                          
    sequence text,                                                                                                                        
    missed_cleavages smallint,
    aa_counts list<smallint>,
    proteins set<text>,
    length smallint,
    is_swiss_prot boolean,
    is_trembl boolean,
    taxonomy_ids set<int>,
    unique_taxonomy_ids set<int>,
    proteome_ids set<text>,
    is_metadata_updated boolean,
    PRIMARY KEY (partition, mass, sequence)
);