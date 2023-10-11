# MaCPepDB - Mass Centric Peptide Database

MaCPepDB creates and maintains a peptide database and provides a web API for accessing the database.

## Ambiguous amino acids
Some UniProt entries contain one letter codes which encode multiple amino acids. Usually the encoded amino acids
have a similar or equal mass. Ambiguous one letter codes are:

* `B` encodes `D` & `N`
* `J` encodes `I` & `L`
* `Z` encodes `E` & `Q`

Because the amino acids encoded by `B` & `Z` have a different mass and only a few hundreds entries contain these, MaCPepDB resolves the ambiguity by creating all possible combination of the peptide with the distinct amino acids, e.g.:   

| ambiguous peptide | distinct peptides |
| --- | --- |
| `PE_B_TIDE_Z_K` | `PE_D_TIDE_E_K` |
|                 | `PE_D_TIDE_Q_K` |
|                 | `PE_N_TIDE_E_K` |
|                 | `PE_N_TIDE_Q_K` |

`J` encodes Leucine and Isoleucine, both have the same mass. Resolving those would not make the peptides better distinguishable by mass.

In theory `X` is also ambiguous, encoding **all** amino acids. Practically a lot more entries containing `X` than the previous mentioned ambiguous amino acids, sometimes with a high abundance of `X`. Resolving these would increase the amount of peptides significantly and slowing down MaCPepDB's search functionality.

## Input
* Protein data are provided by UniProt Text/EMBL format (
    * [Documentation](https://web.expasy.org/docs/userman.html)
    * [Source](https://ftp.expasy.org/databases/uniprot/) (dat-files) or on UniProt download as `Text`
* Taxonomy data are provided
    * [Documentation](https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump_readme.txt)
    * [Source](https://ftp.ncbi.nih.gov/pub/taxonomy/) (taxdump.zip)
)

## Database structure
MaCPepDB utilizes a denormalized database structure for efficient record storage and compatibility with various database engines. Denormalization consolidates data into fewer tables, minimizing redundant data such as multi-column primary key of peptides. However, this approach may result in a loss of certain SQL functionalities, such as foreign keys and cascade operations that ensure database integrity. To compensate for this, the lost functionality is implemented in the application code. Manual alterations to the database are discouraged and should be done using MaCPepDB.

### Configuration
Table: config   

| Column | Type | Data |
| --- | --- | --- |
| conf_key | String (max. length 256) | key to find configuration value |
| value | JSON | Value of configuration key wrapped in a JSON object: `{"wrapper": value>}`

### Proteins
Table: proteins   

| Column | Type | Data |
| --- | --- | --- |
| **accession** | String (max. length 10) | Primary accession in UniProt |
| secondary_accessions | Array of strings (max. string length 10) | Secondary or old accessions after merges |
| entry_name | String (max. length 16) | Entry name in UniProt |
| name | String | Human readable name |
| genes | Array of strings | Containing genes |
| taxonomy_id | Integer | Taxonomy ID |
| proteome_id | String (max. length 11) |  Proteome ID |
| is_reviewed | Boolean | `true` if SwissProt otherwise `false` |
| sequence | String | Amino acid sequence |
| updated_at | Integer | Last entry update in UniProt (Unix timestamp) |

\* primary key is bold

### Peptides
| Column | Type | Data |
| --- | --- | --- |
| **partition** | Integer | A partition key based on the mass to cluster peptides |
| **mass** | Integer | Theoretical mass as integer |
| **sequence** | String (max. length 60) | Amino acid sequence |
| missed_cleavages | Integer | Number of missed cleavages |
| aa_counts | 1 dimension integer array with 26 elements |  Containing the amino acid counts  |
| proteins | 1 dimensional string array | Primary accession of containing proteins | 
| is_swiss_prot | Boolean | `true` if contained by a protein in SwissProt |
| is_trembl | Boolean | `true` if contained by a protein in TrEMBL |
| taxonomy_ids | 1 dimensional integer array | IDs of containing taxonomies |
| unique_taxonomy_ids | 1 dimensional integer array | IDs of taxonomies with only one protein containing this peptide |
| proteome_ids | 1 dimensional string array (max. string length 11) | IDs of containing proteomes |
| is_metadata_updated | Boolean | `true` if metadata is up to date (internal use only) |

\* primary key is bold

### Supported database engines
* [Citus Data](https://www.citusdata.com/)

## Usage

### Web API
cargo run -r web scylla://<COMMA_SEPARATED_SCYLLA_NODES_LIST> <IP> <PORT>

## Citation

**MaCPepDB: A Database to Quickly Access All Tryptic Peptides of the UniProtKB**   
Julian Uszkoreit, Dirk Winkelhardt, Katalin Barkovits, Maximilian Wulf, Sascha Roocke, Katrin Marcus, and Martin Eisenacher   
Journal of Proteome Research 2021 20 (4), 2145-2150    
DOI: 10.1021/acs.jproteome.0c00967 


## Posters about further development
* [MaCPepDB: Increasing the performance of the mass centric peptide database with old hardware and a distributed database](https://macpepdb.mpc.rub.de/api/documents/20220314-macpepdb__increasing-performance.pdf)
* [Enhancements of MaCPepDB – the Mass Centric Peptide Database](https://macpepdb.mpc.rub.de/api/documents/20220331-enhancement_of_macpepdb.pdf)