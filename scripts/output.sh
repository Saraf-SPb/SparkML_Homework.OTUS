awk -F ',' 'NR > 1 { print $1 "," $2 "," $3 "," $4 }' < /scripts/iris.csv | xargs -I % sh -c '{ echo %; sleep 0.5; }'
