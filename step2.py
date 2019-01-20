import os
import subprocess
import shutil
import pyodbc
import pymysql
import numpy as np
import sys


file100_path='file100'
century_path=os.path.join(file100_path,'century_46.exe')
list_path=os.path.join(file100_path,'list100_46.exe')
connection = pymysql.connect(host='localhost',
                             user='root',
                             port=3306,
                             password='1234',
                             database='climate_database',
                             local_infile=True)
connection2 = pymysql.connect(host='localhost',
                             user='root',
                             port=3306,
                             password='1234',
                             database='climate_results',
                             local_infile=True)

def century_run(target_path, bin_output_name):
    p = subprocess.Popen(['cmd', '/C', 'century_46 -s %s -n %s'%('new',bin_output_name)], cwd=target_path)
    p.communicate()
    #the .lis file name is same as .bin
    q = subprocess.Popen(['cmd', '/C', 'list100_46 %s %s outvar.txt' % (bin_output_name, bin_output_name)], cwd=target_path)
    q.communicate()


class Database:
    def __init__(self,database_name,db_path):
        databasename = database_name.split('.')[0]
        self.region_id = databasename.split('_')[2]
        f_path=os.path.join(file100_path,self.region_id)
        f100_new = os.path.join(db_path,databasename)
        try:
            shutil.copytree(f_path, f100_new)
        except:
            print('file exist')
        self.file_path=f100_new
        conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb)};'
                r'DBQ=%s;' % os.path.join(db_path,database_name)
        )
        self.cnxn = pyodbc.connect(conn_str)
        self.crsr = self.cnxn.cursor()
        self.weather_id= None
        self.wth_data = None
        site_qry = 'SELECT DISTINCT ID FROM [Control Records]'
        self.site_data=self.crsr.execute(site_qry).fetchall()
        self.site_id=None
        self.soil_id=None
        self.climate_data=None
        self.output_table_name=None

    def specify_the_climate(self,climate):
        self.climate_data=climate
        self.output_table_name=self.climate_data+'_'+self.region_id

    def extract_weather_data(self,climate_data):
        qry='SELECT [Weather ID],[Soil ID] FROM [Control Records] WHERE ID=%d'%self.site_id
        self.crsr.execute(qry)
        data=self.crsr.fetchall()
        self.weather_id=data[0][0]
        self.soil_id=data[0][1]
        with connection.cursor() as cursor:
            weather_qry = 'SELECT Year,PRECIP,TMN2M,TMX2M FROM `%s` WHERE ID=%d' % (climate_data, self.weather_id)
            cursor.execute(weather_qry)
            wth_data = cursor.fetchall()
            wth_data = sorted(wth_data, key=lambda x: x[0])
            wth_data = np.asarray(wth_data)
            wth_data = np.reshape(wth_data, (-1, 12, 4))
            wth_data = wth_data.transpose(0, 2, 1)
            year = wth_data[:, 0, 0]
            year = np.reshape(year, (-1, 1))
            year = np.concatenate([year] * 3, axis=1).reshape((-1, 1))
            wth_data = wth_data[:, 1:, :]
            wth_data = np.reshape(wth_data, (-1, wth_data.shape[-1]))
            self.wth_data = np.concatenate([year, wth_data], axis=1)
        return self.wth_data

    def write_sch_file(self):
        sch = os.path.join(self.file_path,'sch.txt')
        with open(sch, 'r') as file:
            content = file.readlines()
            for i, line in enumerate(content):
                if i == 0:
                    content[i] = '#%d\n' % self.site_id
                if i == 70:
                    content[i] = 'ic0%s.wth\n' % '{0:>{align}5}'.format(self.weather_id, align='0')
        newsch=os.path.join(self.file_path,'new.sch')
        with open(newsch, 'w') as file:
            for i, line in enumerate(content):
                file.write(line)

    def write_wth_file(self):
        weather_data =''
        for i,row in enumerate(self.wth_data):
            for j,item in enumerate(row):
                if j!=0:
                    weather_data +='{:>7}'.format('%.2f' % item)
                else:
                    if (i+1)%3==1:
                        weather_data +='prec'+'{:>6}'.format('%d'%item)
                    if (i+1)%3==2:
                        weather_data +='tmin'+'{:>6}'.format('%d'%item)
                    if (i+1)%3==0:
                        weather_data +='tmax'+'{:>6}'.format('%d'%item)
            weather_data+='\n'
        wthfile=os.path.join(self.file_path,'ic0%s.wth'%'{0:>{align}5}'.format(self.weather_id, align='0'))
        with open(wthfile, 'w') as f:
             f.write(weather_data)
        return wthfile


    def write_site_file(self):
        cp_qry = 'select * from [Weather Averages] where ID=%d' % self.weather_id
        climate_parameter = self.crsr.execute(cp_qry).fetchall()
        climate_parameter = np.asarray(climate_parameter)
        climate_parameter = np.transpose(climate_parameter)
        l_qry = 'select Latitude, Longitude from LUF_soil_climate where ID=%d'%self.site_id
        l = self.crsr.execute(l_qry).fetchall()
        soil_qry='select * from Soils where [Soil ID]=%d'%self.soil_id
        soil=self.crsr.execute(soil_qry).fetchall()
        soil=soil[0]
        site=os.path.join(self.file_path,'site.txt')
        with open(site, 'r') as file:
            content = file.readlines()
            for i, line in enumerate(content):
                if i == 0:
                    content[i] = 'X  Archived site file record. ID=%d\n' % self.site_id
        newsite=os.path.join(self.file_path,'icsite.100')
        with open(newsite, 'w') as newfile:
            newfile.write(content[0])
            newfile.write(content[1])
            for m in range(1,13):
                item=climate_parameter[2][m-1]
                newline = '{:<15.5f}PRECIP({})\n'.format(item, m)
                newfile.write(newline)
            for m in range(1, 13):
                newfile.write('{:<15.5f}PRCSTD({})\n'.format(0, m))
            for m in range(1, 13):
                newfile.write('{:<15.5f}PRCSKW({})\n'.format(0, m))
            for m in range(1, 13):
                item = climate_parameter[5][m - 1]
                newline = '{:<15.5f}TMN2M({})\n'.format(item, m)
                newfile.write(newline)
            for m in range(1, 13):
                item = climate_parameter[6][m - 1]
                newline = '{:<15.5f}TMX2M({})\n'.format(item, m)
                newfile.write(newline)
            newfile.write(content[62])
            newfile.write(content[63])
            newfile.write(content[64])
            newfile.write('{:<15.5f}SITLAT\n'.format(l[0][0]))
            newfile.write('{:<15.5f}SITLNG\n'.format(l[0][1]))
            newfile.write('{:<15.5f}SAND\n'.format(soil[4]))
            newfile.write('{:<15.5f}SILT\n'.format(soil[5]))
            newfile.write('{:<15.5f}CLAY\n'.format(soil[6]))
            newfile.write('{:<15.5f}ROCK\n'.format(soil[7]))
            newfile.write('{:<15.5f}BULKD\n'.format(soil[8]))
            newfile.write('{:<15d}NLAYER\n'.format(soil[9]))
            if soil[10]==None: soil[10]=0
            newfile.write('{:<15d}NLAYPG\n'.format(soil[10]))
            newfile.write('{:<15.5f}DRAIN\n'.format(soil[11]))
            newfile.write('{:<15.5f}BASEF\n'.format(soil[12]))
            newfile.write('{:<15.5f}STORMF\n'.format(soil[13]))
            newfile.write('{:<15.5f}PRECRO\n'.format(soil[14]))
            newfile.write('{:<15.5f}FRACRO\n'.format(soil[15]))
            newfile.write('{:<15d}SWFLAG\n'.format(soil[16]))
            for i in range(1,11):
                newfile.write('{:<15.5f}AWILT({})\n'.format(0,i))
            for i in range(1,11):
                newfile.write('{:<15.5f}AFIEL({})\n'.format(0,i))
            newfile.write('{:<15.5f}PH\n'.format(soil[17]))
            newfile.write('{:<15.5f}PSLSRB\n'.format(soil[18]))
            newfile.write('{:<15.5f}SORPMX\n'.format(soil[19]))
            for i in range(103,245):
                newfile.write(content[i])


    def run_the_site(self):
        self.extract_weather_data(self.climate_data)
        file_name=self.write_wth_file()
        self.write_sch_file()
        self.write_site_file()
        century_run(self.file_path, 'ic%d' % self.site_id)
        bin_name=os.path.join(self.file_path, 'ic%d.bin'%self.site_id)
        lis_name=os.path.join(self.file_path,'ic%d.lis'%self.site_id)
        if os.path.isfile(file_name):
            os.remove(file_name)
        if os.path.isfile(bin_name):
            os.remove(bin_name)
        with open(lis_name,'r') as output, open(os.path.join(self.file_path, 'output.txt'),'w') as outfile:
            output_file = output.readlines()
            output_file = output_file[3:]
            for data in output_file:
                data = data.split()
                data = str(self.site_id)+','+','.join(data)+'\n'
                outfile.write(data)
        if os.path.isfile(lis_name):
            os.remove(lis_name)

    def create_output_table_mysql(self):
        outvar_path = os.path.join(self.file_path, 'outvar.txt')
        with open(outvar_path, 'r') as var:
            outvar = []
            for i in var:
                outvar.append(i.rstrip("\n"))
            outvar[19:22] = ['`'+i+'`' for i in outvar[19:22]]
            outvar = tuple(outvar[0:22])
        with connection2.cursor() as cursor:
            ostr = ''
            for i in range(len(outvar)):
                ostr += ',%s decimal(32,16)'
            qry1 = "create table `{}` ({} int,{} int {})".format(self.output_table_name,'SiteID','Year',ostr)%outvar
            cursor.execute(qry1)

    def save_outputs_to_mysql(self):
        with connection2.cursor() as cursor:
            cursor.execute('SET GLOBAL local_infile = 1')
            outf=os.path.join(self.file_path, 'output.txt')
            outf='/'.join(outf.split('\\'))
            qry ='''load data local infile '{}' into table `{}` fields terminated by ',' '''.format(outf,self.output_table_name)
            cursor.execute(qry)
            connection2.commit()

    def run_all_sites(self):
        try:
            self.create_output_table_mysql()
            print('create the output table')
        except:
            print('load data from %s'%self.climate_data)
        for site in self.site_data:
            self.site_id = site[0]
            self.run_the_site()
            self.save_outputs_to_mysql()
        print('%s run success'%self.climate_data)


def list_of_climate_files(SCA, RCP26):
    with connection.cursor() as cursor:
        cursor.execute('show tables;')
        data = cursor.fetchall()
        data = list(data)
        data = [item[0] for item in data]
    names = [f for f in data if SCA in f]
    if RCP26 is True:
        names = [f for f in names if 'RCP26' in f]
    else:
        names = [f for f in names if 'RCP85' in f]
    return names


def complete_operations_infolder(num,numr):
    project_sublist = os.listdir(project_path)
    for sca in project_sublist[num-1:num]:
        sca_path = os.path.join(project_path,sca)
        sca_sublist = os.listdir(sca_path)
        for region in sca_sublist[numr-1:numr]:
            region_path=os.path.join(sca_path,region)
            complete_in_region(sca,region_path)

def complete_in_region(sca, region_path):
    region_sublist = os.listdir(region_path)
    for RCP in region_sublist:
        if RCP.endswith('.mdb') and 'RCP85' in RCP:
            RCP26 = 'RCP26' in RCP
            climate_files = list_of_climate_files(sca, RCP26)
            the_database = Database(RCP, region_path)
            for model in climate_files:
                model = model.split('.')[0]
                the_database.specify_the_climate(model)
                #the_database.run_all_sites()
#sca and number of region1-5
#project_path='E:\\ESA_Century_Runs_Climate_Change_Project\\'


# path to sample input folder
project_path = 'E:\\WD Backup.swstor\\yyu3\\YTQyNjVmODIyYzMyNDJmOT\\Volume{e662c559-0000-0000-0000-100000000000}\\ESA_Century_Runs_Climate_Change_Project\\'
complete_operations_infolder(7, 1)

database_sca6 = Database(RCP,)