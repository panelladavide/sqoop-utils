import os
# crea la cartella results dove verranno inseriti i file di output
output_dir = "results"
os.system("mkdir -p "+output_dir)
# instanzia il path dove va a a leggere i files contenenti i comandi sqoop
path = "/root/scripts/python/prova/sqoop_command/"
# Legge ogni file nella cartella
for filename in os.listdir(path):
    print(path+filename)
    with open(path+filename, "r") as f:
        # Legge ogni riga del file
        # Fa un replace della stringa _tmp _tmp1 utile per test, rimuovere se si vuole far girare senza eseguire il test
        # Fa un replace della newlines con uno spazio in modo da non avere a capo durante nel comando bash
        data = f.read().replace('_tmp', '_tmp1').replace('\n', ' ')
        # crea il cmd e appende il log nel file in tmp/log
        sqoop_cmd = data + """ &> tmp/log"""
        print(sqoop_cmd)
        # crea il comando bash da eseguire alla fine
        # il comando si divide in tre parti
        # 1 - si ricava il nome della tabella target dal comando sqoop
        # 2 - esegue il comando sqoop
        # 3 - elabora il log e appende nel file total_output.csv il risultato ricavato dal log di sqoop
        bash_template_cmd = """
         
        # NOME TABELLA
        table_name=($(echo "%s" | awk -F"--hive-table" '{print $2}' | awk -F"--target-dir" '{print $1}'))
        
        echo "###### ESECUZIONE COMANDO SQOOP #####"
        %s
        
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
        tr ' ' ',' < tmp/formatted_output > %s/output_$table_name.csv
        echo "append to total_output.csv"
        sed 1d %s/output_$table_name.csv >> %s/total_output.csv
        """

        bash_cmd = bash_template_cmd % (sqoop_cmd, sqoop_cmd, output_dir,output_dir, output_dir)
        #print(bash_cmd)
        os.system(bash_cmd)
    f.close()
