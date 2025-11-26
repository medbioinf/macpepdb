# MaCPepDB - Mass Centric Peptide Database

MaCPepDB creates and maintains a peptide database and provides a web API for accessing the database.

Note: This is the next iteration of MaCPepDB, moving from the [previous Python-implementation](https://github.com/mpc-bioinformatics/macpepdb) to Rust and using ScyllaDB instead of PostgreSQL / Citus, in order to get better performance and make MaCPepDB ready to include more data.

## Ambiguous amino acids
This was a feature in MaCPepDB Gen 2 and is currently missing in Gen3 as it lead to some confusion.

```text
Some UniProt entries contain one letter codes which encode multiple amino acids. Usually the encoded amino acids
have a similar or equal mass. Ambiguous one letter codes are:

* `B` encodes `D` & `N`
* `J` encodes `I` & `L`
* `Z` encodes `E` & `Q`

Because the amino acids encoded by `B` & `Z` have a different mass and only a few hundreds entries contain these, MaCPepDB resolves the ambiguity by creating all possible combination of the peptide with the distinct amino acids, e.g.:

| ambiguous peptide | distinct peptides |
| ----------------- | ----------------- |
| `PE_B_TIDE_Z_K`   | `PE_D_TIDE_E_K`   |
|                   | `PE_D_TIDE_Q_K`   |
|                   | `PE_N_TIDE_E_K`   |
|                   | `PE_N_TIDE_Q_K`   |

`J` encodes Leucine and Isoleucine, both have the same mass. Resolving those would not make the peptides better distinguishable by mass.

In theory `X` is also ambiguous, encoding **all** amino acids. Practically a lot more entries containing `X` than the previous mentioned ambiguous amino acids, sometimes with a high abundance of `X`. Resolving these would increase the amount of peptides significantly and slowing down MaCPepDB's search functionality.
```

## Input

* Protein data are provided by UniProt Text/EMBL format (
  * [Documentation](https://web.expasy.org/docs/userman.html)
  * [Source](https://ftp.expasy.org/databases/uniprot/) (dat-files) or on UniProt download as `Text`
* Taxonomy data are provided
  * [Documentation](https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump_readme.txt)
  * [Source](https://ftp.ncbi.nih.gov/pub/taxonomy/) (taxdump.zip)

## Database structure

MaCPepDB utilizes a denormalized database structure for efficient record storage and compatibility with various database engines. Denormalization consolidates data into fewer tables, minimizing redundant data such as multi-column primary key of peptides. However, this approach may result in a loss of certain SQL functionalities, such as foreign keys and cascade operations that ensure database integrity. To compensate for this, the lost functionality is implemented in the application code. Manual alterations to the database are discouraged and should be done using MaCPepDB.

### Configuration

Table: config

| Column   | Type                       | Data                                                                         |
| -------- | -------------------------- | ---------------------------------------------------------------------------- |
| conf_key | `text` (max. length 256)   | key to find configuration value                                              |
| value    | `text` (JSON formatted)    | Value of configuration key wrapped in a JSON object: `{"wrapper": value>}`   |

### Proteins

Table: proteins

| Column               | Type                  | Data                                          |
| -------------------- | --------------------- | --------------------------------------------- |
| **accession**        | `text`                | Primary accession in UniProt                  |
| secondary_accessions | `list<text>`          | Secondary or old accessions after merges      |
| entry_name           | `text`                | Entry name in UniProt                         |
| name                 | `text`                | Human readable name                           |
| genes                | `list<text>`          | Containing genes                              |
| taxonomy_id          | `bigint`              | Taxonomy ID                                   |
| proteome_id          | `text`                | Proteome ID                                   |
| is_reviewed          | `boolean`             | `true` if SwissProt otherwise `false`         |
| sequence             | `text`                | Amino acid sequence                           |
| updated_at           | `bigint`              | Last entry update in UniProt (Unix timestamp) |
| domains              | `frozen<set<domain>>` | Set of domains associated with the protein    |

\* primary key is bold

### Peptides

| Column              | Type                  | Data                                                            |
| ------------------- | --------------------- | --------------------------------------------------------------- |
| **partition**       | `bigint`              | A partition key based on the mass to cluster peptides           |
| **mass**            | `bigint`              | Theoretical mass as integer                                     |
| **sequence**        | `text`                | Amino acid sequence                                             |
| missed_cleavages    | `smallint`            | Number of missed cleavages                                      |
| aa_counts           | `list<smallint>`      | Containing the amino acid counts                                |
| proteins            | `set<text>`           | Primary accession of containing proteins                        |
| is_swiss_prot       | `boolean`             | `true` if contained by a protein in SwissProt                   |
| is_trembl           | `boolean`             | `true` if contained by a protein in TrEMBL                      |
| taxonomy_ids        | `set<bigint>`         | IDs of containing taxonomies                                    |
| unique_taxonomy_ids | `set<bigint>`         | IDs of taxonomies with only one protein containing this peptide |
| proteome_ids        | `set<text>`           | IDs of containing proteomes                                     |
| domains             | `frozen<set<domain>>` | Set of domains in which the peptide occured                     |
| is_metadata_updated | `boolean`             | `true` if metadata is up to date (internal use only)            |

\* primary key is bold

### Domain

This is a ScyllaDB user defined datatype.

| Column              | Type     | Data                                                                                                                                                 |
| ------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                | `text`   | Name                                                                                                                                                 |
| evidence            | `text`   | Evidence reference                                                                                                                                   |
| start_index         | `bigint` | Start offset at which the domain begins (relative to the overall protein/peptide sequence)                                                           |
| end_index           | `bigint` | End index (^)                                                                                                                                        |
| protein             | `text`   | The protein accession of the protein that contains the peptide and domain that is within the peptide range (this column empty for the Protein table) |
| start_index_protein | `bigint` | Start index of domain for the protein sequence (this column empty for the Protein table)                                                             |
| end_index_protein   | `bigint` | End index of domain for the protein sequence (this column empty for the Protein table)                                                               |
| peptide_offset      | `bigint` | Start index of the peptide sequence within the protein           

### Blobs

Table to store several binary data. Scylla blob type has in practice a limit of < 1MB, therefore data is stored in multiple chunks of max 512KB. Each chunk has a prefix and chunk index as key.

| Column               | Type                  | Data                                      |
| -------------------- | --------------------- | ----------------------------------------- |
| **key**              | `string`              | `prefix`_`<CHUNK_NUMBER>`                 |
| data                 | `blob`                | Chunk of binary data with max. 512 KB     |    


\* primary key is bold

## Scylla Cluster Deployment

### Install Scylla

Install Scylla 6.2. This is the last OSS release. 

### Configure

For every node in the cluster:

Edit `/etc/scylla/scylla.yaml` and

* set the seed to the IP of the first node in the cluster
* Set listen-address and rpm-address to the IP of the current node
* Set the same cluster name
* Use GossipingPropertyFileSnitch as a snitch

Edit `/etc/scylla/cassandra-rackdc.properties` and

* uncomment and name dc and rack

### Scylla Setup

For every node in the cluster:

* Run `scylla_setup` and use XFS setup on the desired drive
* Don’t use io tuner during setup
* Enable dev mode: `sudo scylla_dev_mode_setup —developer_mode 1`
* `sudo systemctl start scylla-server`

You can try using io tuner for your disk, however in some cases the performance with io tuner and non-devmode seems worse than performance with no iotuner and devmode enabled

## Usage

### Build
To build the database use `cargo run -r -- build ...`. Use `--help` for all options.

### Database URL
Connection settings to database given via an URL: `scylla://[user:password@]<comma_separated_list_of_hosts>/<keyspace>[?attirbute1=foo&attribute2=bar&...]`

Supported attributes (see: <https://docs.rs/scylla/latest/scylla/transport/session_builder/type.SessionBuilder.html>)

| Attribute | Type | Description |
| --- | --- | --- |
| `connection_timeout`| `usize` | In seconds |
| `pool_size` | `usize` |   |
| `pool_type` | `String` | Per host or per shard, only applied when pool_size is given (possible values: `host`, `shard`, default: `host`) |


#### Recommendations
1. Use MaCPepDB's sub commands `mass-count` and `partitioning` to generate balanced peptide partitions before building the database. Once the masses are counted you can use the resulting file to generate different peptide partitions you can use with the build command to find an optimum. E.g. for complete Uniprot a couple million peptides per partitions were the optimum. Depending on your hardware and protein collection it will change.


#### Impact of protease
Most metrics you encounter are obtained using Trypsin for digestion. Be aware that a different protease will generate other metrics. E.g. digesting the Mus Musculus proteome using Trypsin and Unspecific produce (obviously) very different results. Better hardware and much more disc space is required for an unspecific digest as on can imagine.

| protease | peptides | distinct masses |
| --- | --- | --- |
| trypsin | 421062 | 359760 |
| unspecific | 84759305 | 37876354 |

### Web API

`cargo run -r web scylla://<COMMA_SEPARATED_SCYLLA_NODES_LIST>/<KEYSPACE/DATABASE> <IP> <PORT>`

## Citation

**MaCPepDB: A Database to Quickly Access All Tryptic Peptides of the UniProtKB**  
Julian Uszkoreit, Dirk Winkelhardt, Katalin Barkovits, Maximilian Wulf, Sascha Roocke, Katrin Marcus, and Martin Eisenacher  
Journal of Proteome Research 2021 20 (4), 2145-2150  
DOI: 10.1021/acs.jproteome.0c00967

## Posters about further development

* [MaCPepDB: Increasing the performance of the mass centric peptide database with old hardware and a distributed database](https://macpepdb.mpc.rub.de/api/documents/20220314-macpepdb__increasing-performance.pdf)
* [Enhancements of MaCPepDB – the Mass Centric Peptide Database](https://macpepdb.mpc.rub.de/api/documents/20220331-enhancement_of_macpepdb.pdf)
