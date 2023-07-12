### 用于处理2021年-2023年的temp16,temp17,temp18,temp19的数据进行处理 ###

import pandas as pd

def clear_temp(data4,data3,data2,data1):
    ### 对06数据进行重采样 ###
    ### 包含 num = 32 个传感器数据 ###
    data = []
    num = 32
    for i in range(len(data4)-1):
        if data4['id'][i] == 106000 and data4['id'][i+31] == 106031:
            data.append([data4['time'][i],data4['temp'][i],data4['temp'][i+1],data4['temp'][i+2],data4['temp'][i+3],data4['temp'][i+4],data4['temp'][i+5],data4['temp'][i+6],data4['temp'][i+7],data4['temp'][i+8],data4['temp'][i+9],data4['temp'][i+10],data4['temp'][i+11],data4['temp'][i+12],data4['temp'][i+13],data4['temp'][i+14],data4['temp'][i+15],data4['temp'][i+16],data4['temp'][i+17],data4['temp'][i+18],data4['temp'][i+19],data4['temp'][i+20],data4['temp'][i+21],data4['temp'][i+22],data4['temp'][i+23],data4['temp'][i+24],data4['temp'][i+25],data4['temp'][i+26],data4['temp'][i+27],data4['temp'][i+28],data4['temp'][i+29],data4['temp'][i+30],data4['temp'][i+31]])
        else:
            continue
    name = ['time','106000','106001','106002','106003','106004','106005','106006','106007','106008','106009','106010','106011','106012','106013','106014','106015','106016','106017','106018','106019','106020','106021','106022','106023','106024','106025','106026','106027','106028','106029','106030','106031']
    num16_temp = pd.DataFrame(columns=name, data = data).set_index(['time'])
    num16_temp.index = pd.to_datetime(num16_temp.index)

    temp_num16 = num16_temp.resample('5Min').mean()

    ### 对07数据进行重采样 ###
    ### 包含 num = 22 个传感器数据 ###
    data = []
    num = 22
    for i in range(len(data3)-1):
        if data3['id'][i] == 107000 and data3['id'][i+21] == 107021:
            data.append([data3['time'][i],data3['temp'][i],data3['temp'][i+1],data3['temp'][i+2],data3['temp'][i+3],data3['temp'][i+4],data3['temp'][i+5],data3['temp'][i+6],data3['temp'][i+7],data3['temp'][i+8],data3['temp'][i+9],data3['temp'][i+10],data3['temp'][i+11],data3['temp'][i+12],data3['temp'][i+13],data3['temp'][i+14],data3['temp'][i+15],data3['temp'][i+16],data3['temp'][i+17],data3['temp'][i+18],data3['temp'][i+19],data3['temp'][i+20],data3['temp'][i+21]])
        else:
            continue
    name = ['time','107000','107001','107002','107003','107004','107005','107006','107007','107008','107009','107010','107011','107012','107013','107014','107015','107016','107017','107018','107019','107020','107021']
    num17_temp = pd.DataFrame(columns=name, data = data).set_index(['time'])
    num17_temp.index = pd.to_datetime(num17_temp.index)

    temp_num17 = num17_temp.resample('5Min').mean()

    ### 对08数据进行重采样 ###
    ### 包含 num = 32 个传感器数据 ###
    data = []
    num = 32
    for i in range(len(data2)-1):
        if data2['id'][i] == 108000 and data2['id'][i + 31] == 108031:
            data.append([data2['time'][i],data2['temp'][i],data2['temp'][i+1],data2['temp'][i+2],data2['temp'][i+3],data2['temp'][i+4],data2['temp'][i+5],data2['temp'][i+6],data2['temp'][i+7],data2['temp'][i+8],data2['temp'][i+9],data2['temp'][i+10],data2['temp'][i+11],data2['temp'][i+12],data2['temp'][i+13],data2['temp'][i+14],data2['temp'][i+15],data2['temp'][i+16],data2['temp'][i+17],data2['temp'][i+18],data2['temp'][i+19],data2['temp'][i+20],data2['temp'][i+21],data2['temp'][i+22],data2['temp'][i+23],data2['temp'][i+24],data2['temp'][i+25],data2['temp'][i+26],data2['temp'][i+27],data2['temp'][i+28],data2['temp'][i+29],data2['temp'][i+30],data2['temp'][i+31]])
        else:
            continue
    name = ['time','108000','108001','108002','108003','108004','108005','108006','108007','108008','108009','108010','108011','108012','108013','108014','108015','108016','108017','108018','108019','108020','108021','108022','108023','108024','108025','108026','108027','108028','108029','108030','108031']
    num18_temp = pd.DataFrame(columns=name, data = data).set_index(['time'])
    num18_temp.index = pd.to_datetime(num18_temp.index)

    temp_num18 = num18_temp.resample('5Min').mean()

    ### 对09数据进行重采样 ###
    ### 包含 num = 29 个传感器数据 ###
    data = []
    num = 29
    for i in range(len(data1)-1):
        if data1['id'][i] == 109000 and data1['id'][i + 28] == 109028:
            data.append([data1['time'][i],data1['temp'][i],data1['temp'][i+1],data1['temp'][i+2],data1['temp'][i+3],data1['temp'][i+4],data1['temp'][i+5],data1['temp'][i+6],data1['temp'][i+7],data1['temp'][i+8],data1['temp'][i+9],data1['temp'][i+10],data1['temp'][i+11],data1['temp'][i+12],data1['temp'][i+13],data1['temp'][i+14],data1['temp'][i+15],data1['temp'][i+16],data1['temp'][i+17],data1['temp'][i+18],data1['temp'][i+19],data1['temp'][i+20],data1['temp'][i+21],data1['temp'][i+22],data1['temp'][i+23],data1['temp'][i+24],data1['temp'][i+25],data1['temp'][i+26],data1['temp'][i+27],data1['temp'][i+28]])
        else:
            continue
    name = ['time','109000','109001','109002','109003','109004','109005','109006','109007','109008','109009','109010','109011','109012','109013','109014','109015','109016','109017','109018','109019','109020','109021','109022','109023','109024','109025','109026','109027','109028']
    num19_temp = pd.DataFrame(columns=name, data = data).set_index(['time'])
    num19_temp.index = pd.to_datetime(num19_temp.index)

    temp_num19 = num19_temp.resample('5Min').mean()

    ### 合并数据集temp_focal/temp_num16/temp_17/temp_18/temp_19 ###

    df1 = pd.merge(temp_num16,temp_num17,on='time')
    df2 = pd.merge(temp_num18,temp_num19,on='time')
    df3 = pd.merge(df1,df2,on='time')
    return df3