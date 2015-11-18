
#!/bin/bash

# Prepares nanomizer data with Frank
caterr() {
    while IFS= read  line; do
	echo "$line" 1>&2;
    done
}

show_help() {
    caterr <<EOF
Usage: ${0##*/} <minTriples> <maxTriples> <numDatasets> <path>

minTriples: minimum number of triples per dataset
maxTriples: maximum number of triples per dataset
numDatasets: number of datasets to download
path: path to store the datasets
EOF
}

[ "$#" -ne 4 ] && show_help && exit;

for dlink in $(./Frank/frank documents --minTriples $1 --maxTriples $2 | cut -f1 -d' ' | head -n$3)
do
    curl $dlink -o $4/${dlink##*/}.nt.gz;
done

exit 0