
#!/bin/bash

# Prepares nanomizer data with Frank

# show_help() {
#     echo 'foo';
# }

# [ "$#" -eq 0 ] && show_help && exit;

echo $1

for dlink in $(./Frank/frank documents --minTriples 500 --maxTriples 1000 | cut -f1 -d' ' | head -n100)
do
    curl $dlink -o ../data/${dlink##*/}.nt.gz;
done

exit 0