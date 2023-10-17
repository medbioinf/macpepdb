# MaCPepDB - Mass Centric Peptide Database

MaCPepDB creates and maintains a peptide database and provides a web API for accessing the database.

## Ambiguous amino acids

Some UniProt entries contain one letter codes which encode multiple amino acids. Usually the encoded amino acids
have a similar or equal mass. Ambiguous one letter codes are:

- `B` encodes `D` & `N`
- `J` encodes `I` & `L`
- `Z` encodes `E` & `Q`

Because the amino acids encoded by `B` & `Z` have a different mass and only a few hundreds entries contain these, MaCPepDB resolves the ambiguity by creating all possible combination of the peptide with the distinct amino acids, e.g.:

| ambiguous peptide | distinct peptides |
| ----------------- | ----------------- |
| `PE_B_TIDE_Z_K`   | `PE_D_TIDE_E_K`   |
|                   | `PE_D_TIDE_Q_K`   |
|                   | `PE_N_TIDE_E_K`   |
|                   | `PE_N_TIDE_Q_K`   |

`J` encodes Leucine and Isoleucine, both have the same mass. Resolving those would not make the peptides better distinguishable by mass.

In theory `X` is also ambiguous, encoding **all** amino acids. Practically a lot more entries containing `X` than the previous mentioned ambiguous amino acids, sometimes with a high abundance of `X`. Resolving these would increase the amount of peptides significantly and slowing down MaCPepDB's search functionality.

## Input

- Protein data are provided by UniProt Text/EMBL format (
  - [Documentation](https://web.expasy.org/docs/userman.html)
  - [Source](https://ftp.expasy.org/databases/uniprot/) (dat-files) or on UniProt download as `Text`
- Taxonomy data are provided
  _ [Documentation](https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump_readme.txt)
  _ [Source](https://ftp.ncbi.nih.gov/pub/taxonomy/) (taxdump.zip)
  )

## Database structure

MaCPepDB utilizes a denormalized database structure for efficient record storage and compatibility with various database engines. Denormalization consolidates data into fewer tables, minimizing redundant data such as multi-column primary key of peptides. However, this approach may result in a loss of certain SQL functionalities, such as foreign keys and cascade operations that ensure database integrity. To compensate for this, the lost functionality is implemented in the application code. Manual alterations to the database are discouraged and should be done using MaCPepDB.

### Configuration

Table: config

| Column   | Type                     | Data                                                                       |
| -------- | ------------------------ | -------------------------------------------------------------------------- |
| conf_key | String (max. length 256) | key to find configuration value                                            |
| value    | JSON                     | Value of configuration key wrapped in a JSON object: `{"wrapper": value>}` |

### Proteins

Table: proteins

| Column               | Type                | Data                                          |
| -------------------- | ------------------- | --------------------------------------------- |
| **accession**        | text                | Primary accession in UniProt                  |
| secondary_accessions | list<text>          | Secondary or old accessions after merges      |
| entry_name           | text                | Entry name in UniProt                         |
| name                 | text                | Human readable name                           |
| genes                | list<text>          | Containing genes                              |
| taxonomy_id          | bigint              | Taxonomy ID                                   |
| proteome_id          | text                | Proteome ID                                   |
| is_reviewed          | boolean             | `true` if SwissProt otherwise `false`         |
| sequence             | text                | Amino acid sequence                           |
| updated_at           | bigint              | Last entry update in UniProt (Unix timestamp) |
| domains              | frozen<set<Domain>> | Set of domains associated with the protein    |

\* primary key is bold

### Peptides

| Column              | Type                | Data                                                            |
| ------------------- | ------------------- | --------------------------------------------------------------- |
| **partition**       | bigint              | A partition key based on the mass to cluster peptides           |
| **mass**            | bigint              | Theoretical mass as integer                                     |
| **sequence**        | text                | Amino acid sequence                                             |
| missed_cleavages    | smallint            | Number of missed cleavages                                      |
| aa_counts           | list<smallint>      | Containing the amino acid counts                                |
| proteins            | set<text>           | Primary accession of containing proteins                        |
| is_swiss_prot       | boolean             | `true` if contained by a protein in SwissProt                   |
| is_trembl           | boolean             | `true` if contained by a protein in TrEMBL                      |
| taxonomy_ids        | set<bigint>         | IDs of containing taxonomies                                    |
| unique_taxonomy_ids | set<bigint>         | IDs of taxonomies with only one protein containing this peptide |
| proteome_ids        | set<text>           | IDs of containing proteomes                                     |
| domains             | frozen<set<domain>> | Set of domains in which the peptide occured                     |
| is_metadata_updated | Boolean             | `true` if metadata is up to date (internal use only)            |

\* primary key is bold

### Domain

This is a ScyllaDB user defined datatype.

| Column              | Type   | Data                                                                                                                                                 |
| ------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                | text   | Name                                                                                                                                                 |
| evidence            | text   | Evidence reference                                                                                                                                   |
| start_index         | bigint | Start offset at which the domain begins (relative to the overall protein/peptide sequence)                                                           |
| end_index           | bigint | End index (^)                                                                                                                                        |
| protein             | text   | The protein accession of the protein that contains the peptide and domain that is within the peptide range (this column empty for the Protein table) |
| start_index_protein | bigint | Start index of domain for the protein sequence (this column empty for the Protein table)                                                             |
| end_index_protein   | bigint | End index of domain for the protein sequence (this column empty for the Protein table)                                                               |
| peptide_offset      | bigint | Start index of the peptide sequence within the protein                                                                                               |


## Usage

### Web API
`cargo run -r web scylla://<COMMA_SEPARATED_SCYLLA_NODES_LIST> <IP> <PORT>`

## Citation

**MaCPepDB: A Database to Quickly Access All Tryptic Peptides of the UniProtKB**  
Julian Uszkoreit, Dirk Winkelhardt, Katalin Barkovits, Maximilian Wulf, Sascha Roocke, Katrin Marcus, and Martin Eisenacher  
Journal of Proteome Research 2021 20 (4), 2145-2150  
DOI: 10.1021/acs.jproteome.0c00967

## Posters about further development

- [MaCPepDB: Increasing the performance of the mass centric peptide database with old hardware and a distributed database](https://macpepdb.mpc.rub.de/api/documents/20220314-macpepdb__increasing-performance.pdf)
- [Enhancements of MaCPepDB – the Mass Centric Peptide Database](https://macpepdb.mpc.rub.de/api/documents/20220331-enhancement_of_macpepdb.pdf)
