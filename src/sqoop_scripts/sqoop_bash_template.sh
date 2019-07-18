# NOME TABELLA
table_name=($(echo "$SQOOP_CMD" | awk -F"--hive-table" '{print $2}' | awk -F"--target-dir" '{print $1}'))

echo "###### ESECUZIONE COMANDO SQOOP #####"
$SQOOP_CMD

echo "###### ELABORAZIONE OUTPUT #######"

echo "table_name "$table_name > tmp/output
echo "faccio il sed"
sed -n '/completed successfully/,/Transferred/p' tmp/log | sed -n '/Map input records/,/Map output records/p' tmp/log | tr -d '\t\040' | tr '=' ' ' >> tmp/output
echo "faccio il trasposto"
awk '
{
    for (i=1; i<=NF; i++)  {
        a[NR,i] = $i
    }
}
NF>p { p = NF }
END {
    for(j=1; j<=p; j++) {
        str=a[1,j]
        for(i=2; i<=NR; i++){
            str=str" "a[i,j];
        }
        print str
    }
}' tmp/output > tmp/formatted_output
echo "creo il file output parziale"
tr ' ' ',' < tmp/formatted_output > $OUTPUT_DIR/output_$table_name.csv
echo "append to total_output.csv"
sed 1d $OUTPUT_DIR/output_$table_name.csv >> $OUTPUT_DIR/total_output.csv