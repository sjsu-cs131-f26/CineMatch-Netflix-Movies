# IMDb Data Card

## Source

Source URL: https://datasets.imdbws.com/

Documentation URL: https://developer.imdb.com/non-commercial-datasets/

The IMDb datasets are provided for non-commercial use only.

## Dataset size and row count

The complete IMDb dataset contains 7 TSV files with a raw size of 10,360,102,312 bytes.
This is approximately 10.36 GB
The selected file is title.principals.tsv with 101,892,809 total lines, and 101,892,808 data rows

## File organization
The dataset's seven TSV files are:
- 'name.basics.tsv'
- 'title.akas.tsv'
- 'title.basics.tsv'
- 'title.crew.tsv'
- 'title.episode.tsv'
- 'title.principals.tsv'
- 'title.ratings.tsv'

The selected file for Sprint 2 is title.principals.tsv

## Commands used for size and row count
The command used to measure the raw size of the dataset was: 
du -sb /mnt/scratch/CS131_jelenag/projects/team06_sec02_fall2026/imdb_data

The command used to count the rows in the selected table was:
wc -l /mnt/scratch/CS131_jelenag/projects/team06_sec02_fall2026/imdb_data/title.principals.tsv

## Format and parsing

- Format: TSV
- Delimiter: tab
- Header: yes
- Missing-value marker: `\N`

The tab delimiter lets the fields to be separated reliably using the `cut` command.

## Fields in the selected table

| Field number | Field name | Meaning |

| 1 | `tconst` | IMDb title identifier |
| 2 | `ordering` | Order of the credit within the title |
| 3 | `nconst` | IMDb person identifier |
| 4 | `category` | Type of credit, such as director, actor, writer, or producer |
| 5 | `job` | Specific job description, when available |
| 6 | `characters` | Character name or names, when available |

## Fields used in the Sprint 2 analyses

- Field 4, `category`: first frequency table
- Field 5, `job`: second frequency table
- Field 3, `nconst`: Top-10 entity list
- Field 4, `category`: case-insensitive director filter
- Fields 3 and 4, `nconst` and `category`: deduplicated skinny table
