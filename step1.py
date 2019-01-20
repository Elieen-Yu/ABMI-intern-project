import pyodbc
import os
import shutil
import pymysql

project_path='E:\\ESA_Century_Runs_Climate_Change_Project\\'
data_path='E:\\AB Historic & Climate Change Data_DS_CENTURY\\'
begin_title="CENModel"

with open('file_names') as in_file:
    file_names = [line.strip('\n') for line in in_file]
    for i in range(len(file_names)):
        file_names[i] = file_names[i].split(".")[0]
        file_names[i] = file_names[i].split("_")[2]+ '_'+file_names[i].split("_")[3]
# file names of data source

def list_of_txt_names(begin_title):
    items=os.listdir(data_path)
    names=[f for f in items if f.startswith(begin_title)]
    names=[f for f in names if 'SCA' in f]
    return names
text_files = list_of_txt_names(begin_title)


connection = pymysql.connect(host='localhost',
                             user='root',
                             port=3306,
                             password='1234',
                             database='climate_database')
try:
    with connection.cursor() as cursor:
        for file in text_files[0:1]:
            cursor.execute('create TABLE `{}`'
                       '(ID INT, Year int, Month int,'
                       'PRECIP decimal(4,2),PRCSTD decimal(4,2),PRCSKW decimal(4,2), '
                       'TMN2M decimal(4,2), TMX2M decimal(4,2),'
                       'SOLRAD decimal(4,2), RHUMID decimal(4,2), WINDSP decimal(4,2))'.format(file.split('.')[0]))
            filen=file.split('.')[0]
            qry='''load DATA infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/{}' INTO TABLE `{}` ignore 1 lines (ID,Year,Month,PRECIP,@dummy,@dummy,TMN2M,TMX2M,@dummy,@dummy,@dummy)'''.format(file,filen)
            cursor.execute(qry)
            connection.commit()
finally:
    connection.close()




class Database:

    def __init__(self, database_path):
        conn_str=(
            r'DRIVER={Microsoft Access Driver (*.mdb)};'
            r'DBQ=%s;'%database_path
                )
        self.cnxn=pyodbc.connect(conn_str)
        self.crsr=self.cnxn.cursor()
        self.database_path=database_path

    def update_parameters(self,RCP_26):
        # UPDATE parameters
        # change the CO2RMP to 1
        update_co2rmp = 'UPDATE [Control RecordsCarlos] SET CO2RMP=1'
        self.crsr.execute(update_co2rmp)
        # change the CO2PPM(2) to 450 for RCP26
        update_co2ppm2_450 = 'UPDATE [Control RecordsCarlos] SET [CO2PPM(2)]=450'
        # change the CO2PPM(2) to 600 for RCP85
        update_co2ppm2_600 = 'UPDATE [Control RecordsCarlos] SET [CO2PPM(2)]=600'
        if RCP_26 is True:
            self.crsr.execute(update_co2ppm2_450)
        else:
            self.crsr.execute(update_co2ppm2_600)
        self.crsr.commit()

    def update_queries(self):
        # Update queries
        with open('queries', 'rt') as queries1:
            update_queries1 = queries1.read()
        with open('queries2.txt', 'rt') as queries2:
            update_queries2 = queries2.read()
        #  execution
        self.crsr.execute('DROP TABLE ControlRecordsStep1')
        self.crsr.execute(update_queries1)
        self.crsr.execute('DROP TABLE [Control Records]')
        self.crsr.execute(update_queries2)
        self.crsr.commit()

    def load_data(self,RCP_26):
        # load data
        if RCP_26 is True:
            file_names = [names for names in text_files if 'RCP26' in names]
        else:
            file_names = [names for names in text_files if 'RCP85' in names]
        for name in file_names:
            txt_data_path = os.path.join(data_path, name)
            table_name=name.split('.')[0]
            with open(txt_data_path) as f:
                header = f.readline().split()
                self.crsr.execute('''create table {}
                            (%s int, %s int, %s int, %s double, %s double, %s double, %s double, %s double, %s double, %s double, %s double)
                            '''.format(table_name) % tuple(header))
                for row in f:
                    row_data = row.strip('\n').split('\t')
                    the_data = [int(i) for i in row_data[0:3]] + [float(i) for i in row_data[3:11] if i is not '']
                    self.crsr.execute("""INSERT INTO {} (%s, %s, %s, %s, %s, %s)
                            VALUES (?,?,?,?,?,?)""".format(table_name) % tuple(header[0:4] + header[6:8]), the_data)
                    self.crsr.commit()
                    if the_data[0]%5000 == 0:
                        print(the_data)


def create_database_file (file_path,thefile,create_name):
    new_dir=os.path.join(file_path,thefile.split('.')[0])
    if not os.path.isdir(new_dir):
        os.makedirs(new_dir)
    #target database
    database_selected=os.path.join(file_path,thefile)
    #create a new file
    new_database=os.path.join(file_path,thefile.split('.')[0],create_name)
    shutil.copy2(database_selected,new_database)

def complete_operations_infolder():
    file_list_sca = os.listdir(project_path)
    for sca in file_list_sca:
        sca_path = os.path.join(project_path,sca)
        file_list = os.listdir(sca_path)
        for thefile in file_list:
            if not thefile.endswith('.mdb'):
                region=thefile.split('_')[2]
                fpath=r'E:\file100'
                newpath=os.path.join(fpath,region)
                if not os.path.exists(newpath):
                    os.makedirs(newpath)
                for file in os.listdir(fpath):
                    if file.endswith('.exe'):
                        f=os.path.join(fpath,file)
                        shutil.copy2(f, newpath)
                # for RCP in ['RCP26','RCP85']:
                #     new_name=thefile.split('.')[0]+'_'+RCP+'.mdb'
                #     if RCP=='RCP26':
                #         RCP_26 = True
                #     else: RCP_26 = False
                #     #create_database_file(sca_path, thefile, new_name)
                #     # new_database_operations
                #     new_file_path=os.path.join(sca_path,thefile.split('.')[0],new_name)
                #     new_file = Database(new_file_path)
                #     new_file.update_parameters(RCP_26)
                #     new_file.update_queries()


#complete_operations_infolder()