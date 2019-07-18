import os
import time

# esegue gli script sqoop dai percorsi passati
def execute_sqoop_scripts(file_list, BASE_DIR):

    BASH_TEMPLATE_FILE = os.path.join(BASE_DIR, 'src', 'sqoop_scripts', 'sqoop_bash_template.sh')

    # crea la cartella results dove verranno inseriti i file di output
    current_millis = int(round(time.time() * 1000))
    output_dir = os.path.join(BASE_DIR, "results", str(current_millis))
    os.system("mkdir -p " + output_dir)

    # crea la cartella temporanea
    # TODO: fare in modo che lo script bash utilizzi questa tmp in modo dinamico
    # TODO: creare tmp in questo modo: os.path.join(BASE_DIR, "tmp")
    tmp_dir = os.path.join(os.getcwd(), "tmp")
    os.system("mkdir -p " + tmp_dir)

    # crea il file total_output.csv con l'intestazione
    output_csv = os.path.join(output_dir, 'total_output.csv')
    os.system('echo "table_name,Mapinputrecords,Mapoutputrecords" > ' + output_csv)

    def read_bash_template(file_path):
        with open(file_path, 'r') as template_file:
            return template_file.read() 

    # Cicla la lista passata
    for file in file_list:
        print(file)
        with open(file, "r") as f:
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
            bash_template_cmd = read_bash_template(BASH_TEMPLATE_FILE)

            bash_cmd = bash_template_cmd % (sqoop_cmd, sqoop_cmd, output_dir,output_dir, output_dir)
            #print(bash_cmd)
            os.system(bash_cmd)
        f.close()
