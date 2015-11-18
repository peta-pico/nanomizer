
#!/bin/bash

# Prepares nanomizer data with Frank

for dlink in $(./Frank/frank documents --minTriples 500 --maxTriples 1000 | cut -f1 -d' ' | head -n100)
do
    echo $dlink
done

exit 0