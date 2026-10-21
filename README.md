# MaCPepDB - Mass Centric Peptide Database

MaCPepDB creates and maintains a peptide database and provides a web API for accessing the database.

Note: This is the next iteration of MaCPepDB, moving from the [previous Python-implementation](https://github.com/mpc-bioinformatics/macpepdb) to Rust. It digests UniProt protein files with a protease and stores the resulting peptides in PostgreSQL using the Citus extension, in order to get better performance and make MaCPepDB ready to include more data.

## Input

* Protein data are provided by UniProt Text/EMBL format
  * [Documentation](https://web.expasy.org/docs/userman.html)
  * [Source](https://ftp.expasy.org/databases/uniprot/) (dat-files) or on UniProt download as `Text`
* Taxonomy data are provided from NCBI
  * [Documentation](https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump_readme.txt)
  * [Source](https://ftp.ncbi.nih.gov/pub/taxonomy/) (taxdump.zip)

## Database structure

MaCPepDB utilizes a denormalized database structure for efficient record storage. Denormalization consolidates data into fewer tables, minimizing redundant data such as multi-column primary keys of peptides. However, this approach may result in a loss of certain SQL functionalities, such as foreign keys and cascade operations that ensure database integrity. To compensate for this, the lost functionality is implemented in the application code. Manual alterations to the database are discouraged and should be done using MaCPepDB.

Runtime configuration (protease and mass partitioning) is not a separate table; it's stored as a JSON blob in the `blobs` table under the key `configuration`.

### Proteins

Table: proteins (Citus-distributed by `id`)

| Column          | Type       | Data                                                    |
| --------------- | ---------- | ------------------------------------------------------- |
| **id**          | `integer`  | Internal numeric ID                                     |
| accession       | `text`     | Primary accession in UniProt                            |
| sequence        | `bytea`    | Amino acid sequence, bit-packed                         |
| taxonomy_id     | `integer`  | Taxonomy ID                                             |
| flags           | `"char"`   | Bit flags; bit 0 = is reviewed (SwissProt vs. TrEMBL)   |
| genes           | `text[]`   | Containing genes                                        |

\* primary key is bold

### Peptides

Table: peptides (columnar storage via `citus_columnar`, zstd-compressed, Citus-distributed by `partition`)

| Column                  | Type        | Data                                                                 |
| ----------------------- | ----------- | -------------------------------------------------------------------- |
| partition               | `bigint`    | Partition key based on the mass, to cluster peptides                 |
| mass                    | `bigint`    | Theoretical mass as integer                                          |
| sequence                | `bytea`     | Amino acid sequence, 5-bit-packed, up to 50 amino acids              |
| amino_acid_counts       | `bytea`     | Per-amino-acid counts, each count as one byte                        |
| protein_ids             | `bytea`     | IDs of proteins containing this peptide, "var int" delta encoding    |
| unique_taxonomy_ids     | `integer[]` | IDs of taxonomies with only one protein containing this peptide      |
| non_unique_taxonomy_ids | `integer[]` | IDs of taxonomies with more than one protein containing this peptide |
| flags                   | `"char"`    | Bit flags; bit 0 = is SwissProt, bit 1 = is TrEMBL                   |

The table has no primary key or index — columnar storage doesn't support them. Selective reads instead rely on Citus shard pruning on `partition` plus columnar stripe/chunk-group min/max pruning, which the build keeps effective by loading rows in `(partition, mass)` order, one full columnar stripe per partition.

### Taxonomies

Tables: taxonomies, taxonomy_ranks (regular row-store tables on the coordinator, not Citus-distributed)

| Table           | Column          | Type      | Data                                         |
| --------------- | --------------- | --------- | -------------------------------------------- |
| taxonomy_ranks  | **id**          | `smallint`| Rank ID                                      |
| taxonomy_ranks  | name            | `text`    | Rank name (e.g. species, genus)              |
| taxonomies      | **id**          | `integer` | Taxonomy ID                                  |
| taxonomies      | parent_id       | `integer` | ID of the parent taxonomy                    |
| taxonomies      | scientific_name | `text`    | Scientific name (trigram-indexed for search) |
| taxonomies      | rank_id         | `smallint`| References `taxonomy_ranks.id`               |

\* primary key is bold

### Blobs

Table to store arbitrary binary data (e.g. the runtime configuration, see above). Data is stored in chunks; each chunk is identified by a key and a part number.

Table: blobs (Citus-distributed by `key`)

| Column  | Type       | Data                        |
| ------- | ---------- | --------------------------- |
| **key** | `text`     | Blob identifier             |
| **part**| `smallint` | Chunk index within the blob |
| data    | `bytea`    | Chunk of binary data        |

\* primary key is bold

### Stats

Table: stats (Citus-distributed by `key`)

| Column | Type      | Data                                      |
| ------ | --------- | ----------------------------------------- |
| **key**| `text`    | Stat name                                 |
| value  | `bigint`  | Cached count (e.g. protein/peptide count) |

\* primary key is bold

## Platform support

Pre-built binaries for Linux (x86_64, aarch64), macOS (x86_64, aarch64) and Windows (x86_64) are attached to each [release](https://github.com/medbioinf/macpepdb/releases).

> [!NOTE]
> MaCPepDB is only completely tested on Linux. It needs a running PostgreSQL/Citus cluster, which is only available for Linux in our CI. The macOS and Windows binaries compile and are expected to work, but are not tested against a live database.

## Database deployment

MaCPepDB runs on PostgreSQL with the [Citus](https://www.citusdata.com/) extension. Locally, `docker-compose.yml` brings up a Citus coordinator and worker nodes with `postgres` as distirbuted database

```bash
docker compose up
```

Then initialize the schema (this **drops** any existing MaCPepDB schema):

```bash
psql -h 127.0.0.1 -U postgres -d postgres -f db.sql
```

`db.sql` also applies aggressive build-mode settings (durability off, large WAL, autovacuum off) — safe only because the database is fully rebuildable from the source protein/taxonomy files. After a build, revert those settings, add read-time indexes, and refresh statistics with:

```bash
psql -h 127.0.0.1 -U postgres -d postgres -f db_serve.sql
```

For a production ready environment follow the [Citus documentation](https://learn.microsoft.com/postgresql/citus/).

## Usage
See `cargo run -r --package=macpepdb -- --help` for common arguments, e.g. database URL, logging, etc.

### Build
To build the database use `cargo run -r --package=macpepdb -- build ...`. Use `--help` for all options.

### Database URL
Connection settings to the database are given via a URL: `postgresql://[<user>[:<url-safe-password>]@]<host>[:<port>][,<host>[:<port>]...]/<dbname>[?param=val&...]`

Any standard libpq connection parameter (e.g. `connect_timeout`, `sslmode`) is passed straight through. One MaCPepDB-specific parameter is supported in addition:

| Attribute    | Type    | Description                                           |
| ------------ | ------- | ----------------------------------------------------- |
| `pool_size`  | `usize` | Size of the connection pool (default: 16)             |

#### Impact of protease
Most metrics you encounter are obtained using Trypsin for digestion. Be aware that a different protease will generate other metrics. E.g. digesting the E. Coli (K12) proteome using Trypsin, semi Trypsin and Unspecific produce very different results

| protease     | peptides | distinct masses | peptide table size |
| ------------ | -------- | --------------- | ------------------ |
| trypsin      | 288814   | 253927          | 24 MB              |
| semi-trypsin | 5734909  | 3383505         | 172 MB             |
| unspecific   | 55965824 | 23680825        | 1693 MB            |

 Better hardware and much more disc space might be required depending on the protoeme and protease. 

### Web API

```bash
cargo run -r -- --database-url postgresql://<user>:<pass>@<host>/<dbname> api [socket]
```

`socket` defaults to `127.0.0.1:8080`. Top-level routes:

* `/api/peptides/...` — mass-based peptide search, peptide lookup/existence check
* `/api/proteins/...` — protein lookup/search, ID resolution
* `/api/taxonomies/...` — taxonomy lookup/search, subspecies listing
* `/api/configuration/` — the stored build configuration (protease, mass partitioning)
* `/api/chemistry/...` — amino acid and hydrophobicity lookups

Consult the [controller documentation](https://docs.rs/macpepdb/latest/macpepdb/web/index.html) for detailed description of the individual endpoints. 

### Frontend

Each release contains `macpepdb-frontend-<version>.tar.gz`, a static bundle of the web frontend. Unpack it and serve it with any web server. Unknown paths must fall back to `index.html`, because the frontend uses client-side routing. The frontend reads its settings at runtime from `assets/config.toml` inside the bundle, so one bundle fits every deployment:

```toml
# Base URL of the MaCPepDB web API (`macpepdb api`)
macpepdb_base_url = "http://127.0.0.1:8080"

# Optional: enable Matomo tracking by setting the URL and site ID.
# Add the Matomo URL to the allowed origins (CSP `connect-src`) of the web server.
# matomo_url = "https://matomo.example.org/matomo.php"
# matomo_site_id = 1

# Optional: show the de.NBI survey banner (default: false)
# show_denbi_survey_banner = true
```

Minimal Caddy example (`Caddyfile`), serving the unpacked bundle on port 8888:

```caddyfile
:8888 {
	root * /path/to/macpepdb-frontend-<version>
	try_files {path} {path}/ /index.html
	file_server
}
```

Minimal nginx example (`/etc/nginx/conf.d/default.conf`):

```nginx
server {
    listen 8888;
    root /path/to/macpepdb-frontend-<version>;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

These examples set no security headers. The Docker image's [`Caddyfile`](docker/frontend/Caddyfile) shows a hardened setup with a Content Security Policy.

The API must allow the frontend's origin via CORS if both are served from different origins. See [`packages/frontend/config.template.toml`](packages/frontend/config.template.toml) for the template.

### Ansible deployment

`ansible/` deploys the web stack (API, frontend, Caddy reverse proxy) as Docker containers. Requires Ansible with the `community.docker` collection on the controller and SSH access to the target hosts.

1. Copy the inventory template and fill in every `<PLACEHOLDER>`:

   ```bash
   cp ansible/inventory/template.yml ansible/inventory/<env>.yml
   ```

   | Group      | Purpose                                                                 |
   | ---------- | ----------------------------------------------------------------------- |
   | `common`   | Base host setup                                                         |
   | `docker`   | Docker installation                                                     |
   | `database` | Citus coordinator and workers (`db.yml`; its `database` role is not yet in the repo) |
   | `api`      | MaCPepDB API containers, one per port in `macpepdb_api_ports`           |
   | `frontend` | Frontend container, configured via `macpepdb_public_url`, Matomo, etc.  |
   | `caddy`    | Reverse proxy, domains and redirects via `macpepdb_domains`             |

   `api`, `frontend` and `caddy` variables are read from the first host of their group, so keep them identical within a group. Keep the DB password out of version control, e.g. with `ansible-vault`.

2. Deploy. `macpepdb_action` is required: `start` (re)creates the containers, any other value only stops and removes them.

   ```bash
   cd ansible
   ansible-playbook -i inventory/<env>.yml web.yml -e macpepdb_action=start
   ```

3. Deploy only parts with tags (`common`, `docker`, `api`, `frontend`, `caddy`):

   ```bash
   ansible-playbook -i inventory/<env>.yml web.yml -e macpepdb_action=start --tags api,caddy
   ```

Images are pulled from the registry named in `macpepdb_dockerimage` / `macpepdb_frontend_dockerimage`. To ship a locally built image instead, set the matching `*_dockerimage_file` to a tarball path on the controller. Set `macpepdb_show_denbi_survey_banner` to `~` to hide the survey banner (any other value shows it).

## Releases

Releases are driven by [release-plz](https://release-plz.dev) (`release-plz.toml`, `.github/workflows/release-plz.yml`); no tags are pushed by hand.

1. Every push to `main` opens or updates a release PR with per-crate version bumps and changelogs, derived from [conventional commits](https://www.conventionalcommits.org). Crates without changes (e.g. the leaf crates) are left untouched.
2. Merging the release PR publishes the changed library crates to crates.io. The frontend (`publish = false`) is never published there.
3. The merge also tags `macpepdb` as `X.Y.Z`. The tag triggers `.github/workflows/build.yml`, which builds:
   - release binaries for five targets,
   - the Docker images `ghcr.io/medbioinf/macpepdb` and `ghcr.io/medbioinf/macpepdb-frontend` (tagged `X.Y.Z` and `latest`),
   - the frontend tarball,
   - and a GitHub release with all artifacts and checksums.

Required repository secrets: `CRATESIO_TOKEN` (crates.io publishing) and `RELEASE_PLZ_TOKEN` (PAT or GitHub App token; the default `GITHUB_TOKEN` would not let the tag trigger `build.yml`).

## Citation

**MaCPepDB: A Database to Quickly Access All Tryptic Peptides of the UniProtKB**  
Julian Uszkoreit, Dirk Winkelhardt, Katalin Barkovits, Maximilian Wulf, Sascha Roocke, Katrin Marcus, and Martin Eisenacher  
Journal of Proteome Research 2021 20 (4), 2145-2150  
DOI: 10.1021/acs.jproteome.0c00967
