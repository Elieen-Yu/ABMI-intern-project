import pymysql
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# connection to the mysql database
connection = pymysql.connect(host='localhost',
                             user='root',
                             port=3306,
                             password='1234',
                             database='climate_results',
                             local_infile=True)


def fetch_data (field_name, table_name):
    with connection.cursor() as cursor:
        query1 = '''select avg({}), siteID from `{}` where year >= 4985 and year <= 5009 
                group by siteID '''.format(field_name, table_name)
        query2 = '''select avg({}), siteID from `{}` where year >= 5035 and year <= 5059 
                group by siteID '''.format(field_name, table_name)
        # p1 data
        cursor.execute(query1)
        p1data = cursor.fetchall()
        var_data1 = [item[0] for item in list(p1data)]
        siteid = [item[-1] for item in list(p1data)]
        # p2 data
        cursor.execute(query2)
        p2data = cursor.fetchall()
        var_data2 = [item[0] for item in list(p2data)]
        # absolute change from P1 to P2
        abs_change = [(y - x)*20 for x, y in zip(var_data1, var_data2)]
        # percentage change from P1 to P2
        per_change = [(y - x)/x for x, y in zip(var_data1, var_data2)]
        cmodel = table_name.split('_')[2]
        rcp = table_name.split('_')[3]
        rpara = table_name.split('_')[-1]
        name = '_'.join([cmodel, rcp, rpara])
        data = pd.Series(abs_change, index=siteid, dtype=float, name=name)
    return data


def name_of_database(sca):
    with connection.cursor() as cursor:
        query2 = '''show tables;'''
        cursor.execute(query2)
        name = cursor.fetchall()
        name = list(name)
        name = [item[0] for item in name if sca in item[0]]
    return name


##########SAMPLE PLOTTING#####################
def sample_data(sample_field_names, sca):
    sample_db=name_of_database(sca)
    sample_sca_data = []
    for name in sample_db:
        sample_data = fetch_data(sample_field_names, name)
        sample_sca_data.append(sample_data)
    sample_sca_data = pd.concat(sample_sca_data, axis=1)
    #sample_sca_data = sample_sca_data.fillna(sample_sca_data.mean())
    return sample_sca_data
s = sample_data('totc', 'sca1')
null_columns = s.columns[s.isnull().any()]
a = s[s.isnull().any(axis=1)][null_columns].head()
print a

# select data we want plot
def select_colnames(word, colnames):
    new = [name for name in colnames if word in name]
    return new

# function for setting the colors of the box plots pairs
def set_box_color(bp, color,color2):
    plt.setp(bp['boxes'][0], color=color)
    plt.setp(bp['whiskers'][0], color=color)
    plt.setp(bp['whiskers'][1], color=color)
    plt.setp(bp['caps'][0], color=color)
    plt.setp(bp['caps'][1], color=color)
    plt.setp(bp['medians'][0], color=color)
    plt.setp(bp['boxes'][1], color=color2)
    plt.setp(bp['whiskers'][2], color=color2)
    plt.setp(bp['whiskers'][3], color=color2)
    plt.setp(bp['caps'][2], color=color2)
    plt.setp(bp['caps'][3], color=color2)
    plt.setp(bp['medians'][1], color=color2)


def plot_run_data(run, var, num, r):
    sca = 'sca%s'%num
    sample_sca_data = sample_data(var, sca)
    plt.figure()
    col_rcp26 = select_colnames('rcp26', run)
    col_rcp85 = select_colnames('rcp85', run)
    #boxplot pair
    for index, (col1, col2) in enumerate(zip(col_rcp26, col_rcp85)):
        pos = 1+index*3
        data = [sample_sca_data[col1], sample_sca_data[col2]]
        bp = plt.boxplot(data, positions=[pos, pos + 1], sym='', widths=0.8)
        set_box_color(bp, '#D7191C', '#2C7BB6')

    r_name = [name.split('_')[0] for name in col_rcp26]
    plt.xticks(np.arange(1.5, 26, 3.0), r_name, rotation=45)
    # set axes limits and labels
    #plt.ylim(-5000, 5000)
    plt.xlim(0, 27)
    # draw temporary red and blue lines and use them to create a legend
    plt.plot([], c='#D7191C', label='RCP 2.6')
    plt.plot([], c='#2C7BB6', label='RCP 8.5')
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.xlabel('Climate Models')
    plt.ylabel('abs changes in %s'%var)
    plt.title('SCA%s %d'%(num,r))
    plt.tight_layout()
    plt.subplots_adjust(right=0.75)


def plot_var(var,num):
    #full column names
    sca = 'sca%s'%num
    sample_sca_data = sample_data(var, sca)
    colname = list(sample_sca_data)
    Run = ['r0','r1','r3','r62','r87']
    for i,r in enumerate(Run):
        R = select_colnames(r, colname)
        plot_run_data(R, var,num,r)
        plt.savefig('%s%s.png' % (var, r))
        plt.close()

#plot_var('agcprd',7)

def plot_avgclimate_data(var, num):
    plt.figure()
    sca = 'sca%s'%num
    sample_sca_data = sample_data(var, sca)
    colname = list(sample_sca_data)
    Run = ['r0', 'r1', 'r3', 'r62', 'r87']
    #Run = ['r0', 'r13', 'r3', 'r67', 'r80']
    #Run = ['r0', 'r15', 'r25', 'r3', 'r67']
    #Run = ['r0', 'r15', 'r25', 'r39', 'r7']
    #Run = ['r0', 'r15', 'r25', 'r39', 'r93']
    #Run = ['r0', 'r25', 'r3', 'r39', 'r7']
    #Run = ['r0', 'r25', 'r3', 'r80', 'r87']
    for index,r in enumerate(Run):
        R = select_colnames(r, colname)
        #boxplot pair
        col_rcp26 = select_colnames('rcp26', R)
        col_rcp85 = select_colnames('rcp85', R)
        pos = 1 + index*3
        #average climate models
        mean26 = sample_sca_data[col_rcp26].mean(axis=1)
        mean85 = sample_sca_data[col_rcp85].mean(axis=1)
        bp = plt.boxplot([sample_sca_data[col_rcp26], sample_sca_data[col_rcp85]], positions=[pos, pos + 1], sym='', widths=0.8)
        set_box_color(bp, '#D7191C', '#2C7BB6')

    plt.xticks(np.arange(1.5, 15, 3.0), Run, rotation=45)
    # set axes limits and labels
    #plt.ylim(-5000, 5000)
    plt.xlim(0, 16)
    # draw temporary red and blue lines and use them to create a legend
    plt.plot([], c='#D7191C', label='RCP 2.6')
    plt.plot([], c='#2C7BB6', label='RCP 8.5')
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.ylabel('average absolute changes')
    plt.title('average %s'%var)
    plt.tight_layout()
    plt.subplots_adjust(right=0.75)
    plt.savefig('%s_SCA%d.png'%(var, num))

#plot_avgclimate_data('agcprd',1)


def plot_avgsca_data(title):
    plt.figure()
    name_list = []
    for index in range(1, 10):
        sca = 'sca%d' % index
        name_list.append(sca)
        sca_data = sample_data(title, sca)
        colname = list(sca_data)
        #boxplot pair
        col_rcp26 = select_colnames('rcp26', colname)
        col_rcp85 = select_colnames('rcp85', colname)
        pos = 1 + (index - 1)*3
        #average climate models
        mean26 = sca_data[col_rcp26].mean(axis=1)
        mean85 = sca_data[col_rcp85].mean(axis=1)
        bp = plt.boxplot([mean26, mean85], positions=[pos, pos + 1], sym='', widths=0.8)
        set_box_color(bp, '#D7191C', '#2C7BB6')

    plt.xticks(np.arange(1.5, 27, 3.0), name_list, rotation=45)
    # set axes limits and labels
    #plt.ylim(-5000, 5000)
    plt.xlim(0, 27)
    # draw temporary red and blue lines and use them to create a legend
    plt.plot([], c='#D7191C', label='RCP 2.6')
    plt.plot([], c='#2C7BB6', label='RCP 8.5')
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.ylabel('average absolute changes')
    plt.title(title)
    plt.tight_layout()
    plt.subplots_adjust(right=0.75)
    plt.savefig('sca%s.png'%title)

#plot_avgsca_data('agcprd')