#!/usr/bin/env bash
set -euo pipefail

mkdir -p data
cd data

URL="https://databus.dbpedia.org/dbpedia/generic/wikilinks/2022.12.01/wikilinks_lang=en.ttl.bz2"
if [ ! -f "wikilinks_lang=en.ttl.bz2" ]; then
wget -O wikilinks_lang=en.ttl.bz2 "$URL"
fi

# sample 1%
bzcat wikilinks_lang=en.ttl.bz2 | awk 'BEGIN{srand()} { if (rand() < 0.01) print }' > sample_1pc.ttl
awk '{print $1 "\t" $3}' sample_1pc.ttl | sed 's/[<>]//g' > edges_1pc.tsv

# sample 10%
bzcat wikilinks_lang=en.ttl.bz2 | awk 'BEGIN{srand()} { if (rand() < 0.10) print }' > sample_10pc.ttl
awk '{print $1 "\t" $3}' sample_10pc.ttl | sed 's/[<>]//g' > edges_10pc.tsv
