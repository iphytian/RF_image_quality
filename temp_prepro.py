### 对圆顶温度temp16、17、18、19进行清洗，得到pv温度的PV值和均值 ###

import pandas as pd

### 定义函数判断温度数据是否正常 ###
### 定义函数判断温度数据是否正常 ###
def re_value(value):
    if value < 50 and value > -25:
        value = value
        num = 1
    else:
        value = 0
        num = 0
    return value,num

'''用于计算温度的变化量'''
def custom_resampler(array_like):
    num = len(array_like)
    if num == 0:
        data = ''
    elif num == 1:
        data = ''
    else:
        max_value = array_like['temp_mean'][0]
        min_value = array_like['temp_mean'][0]
        max_index = array_like.index[0]
        min_index = array_like.index[0]
        for i in range(len(array_like)):
            value = array_like['temp_mean'][i]
            index = array_like.index[i]
            if max_value < value:
                max_value = value
                max_index = index
            elif min_value > value:
                min_value = value
                min_index = index
            else:
                continue
        if max_index < min_index:
            data = max_value - min_value
        else:
            data = min_value - max_value
    return data

def clear_pretemp(data):
    new_data_mbjin = []
    new_data_mbyuan = []
    new_data_focalstop = []
    new_data_mbstop = []

    for i in range(len(data)):
        ### 计算Mb的远温度计近温度计的温差 ###
        '''计算近Mb温度均值'''
        temp1, num1 = re_value(data['106000'][i])
        temp2, num2 = re_value(data['106002'][i])
        temp3, num3 = re_value(data['108012'][i])
        temp4, num4 = re_value(data['108014'][i])
        temp5, num5 = re_value(data['106004'][i])
        temp6, num6 = re_value(data['106006'][i])
        temp7, num7 = re_value(data['106008'][i])
        temp8, num8 = re_value(data['108016'][i])
        temp9, num9 = re_value(data['108018'][i])
        temp10, num10 = re_value(data['106010'][i])
        temp11, num11 = re_value(data['106012'][i])
        temp12, num12 = re_value(data['106014'][i])
        temp13, num13 = re_value(data['108020'][i])
        temp14, num14 = re_value(data['108022'][i])
        temp15, num15 = re_value(data['108024'][i])
        temp16, num16 = re_value(data['106018'][i])
        temp17, num17 = re_value(data['106019'][i])
        temp18, num18 = re_value(data['106020'][i])
        temp19, num19 = re_value(data['106016'][i])
        temp20, num20 = re_value(data['108026'][i])
        temp21, num21 = re_value(data['108028'][i])
        temp22, num22 = re_value(data['108030'][i])
        temp23, num23 = re_value(data['106021'][i])
        temp24, num24 = re_value(data['106022'][i])
        temp25, num25 = re_value(data['109024'][i])
        temp26, num26 = re_value(data['109026'][i])
        temp27, num27 = re_value(data['109000'][i])
        temp28, num28 = re_value(data['109002'][i])
        temp29, num29 = re_value(data['106023'][i])
        temp30, num30 = re_value(data['109004'][i])
        temp31, num31 = re_value(data['109006'][i])
        temp32, num32 = re_value(data['109008'][i])
        temp33, num33 = re_value(data['109010'][i])
        temp34, num34 = re_value(data['109012'][i])
        temp35, num35 = re_value(data['109014'][i])
        temp36, num36 = re_value(data['109016'][i])
        temp37, num37 = re_value(data['109018'][i])

        '''计算远Mb温度均值'''
        temp01, num01 = re_value(data['108001'][i])
        temp02, num02 = re_value(data['108000'][i])
        temp03, num03 = re_value(data['108013'][i])
        temp04, num04 = re_value(data['108015'][i])
        temp05, num05 = re_value(data['106005'][i])
        temp06, num06 = re_value(data['106007'][i])
        temp07, num07 = re_value(data['106009'][i])
        temp08, num08 = re_value(data['108017'][i])
        temp09, num09 = re_value(data['108019'][i])
        temp010, num010 = re_value(data['108002'][i])
        temp011, num011 = re_value(data['108003'][i])
        temp012, num012 = re_value(data['108004'][i])
        temp013, num013 = re_value(data['108021'][i])
        temp014, num014 = re_value(data['108023'][i])
        temp015, num015 = re_value(data['108025'][i])
        temp016, num016 = re_value(data['108005'][i])
        temp017, num017 = re_value(data['108006'][i])
        temp018, num018 = re_value(data['108007'][i])
        temp019, num019 = re_value(data['108008'][i])
        temp020, num020 = re_value(data['108027'][i])
        temp021, num021 = re_value(data['108029'][i])
        temp022, num022 = re_value(data['108031'][i])
        temp023, num023 = re_value(data['108009'][i])
        temp024, num024 = re_value(data['108010'][i])
        temp025, num025 = re_value(data['109025'][i])
        temp026, num026 = re_value(data['109027'][i])
        temp027, num027 = re_value(data['109001'][i])
        temp028, num028 = re_value(data['109003'][i])
        temp029, num029 = re_value(data['108011'][i])
        temp030, num030 = re_value(data['109005'][i])
        temp031, num031 = re_value(data['109007'][i])
        temp032, num032 = re_value(data['109009'][i])
        temp033, num033 = re_value(data['109011'][i])
        temp034, num034 = re_value(data['109013'][i])
        temp035, num035 = re_value(data['109015'][i])
        temp036, num036 = re_value(data['109017'][i])
        temp037, num037 = re_value(data['109019'][i])

        '''计算近mb平均温度'''
        jin_mb_num = num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18 + num19 + num20 + num21 + num22 + num23 + num24 + num25 + num26 + num27 + num28 + num29 + num30 + num31 + num32 + num33 + num34 + num35 + num36 + num37
        jin_mb_temp = temp1 + temp2 + temp3 + temp4 + temp5 + temp6 + temp7 + temp8 + temp9 + temp10 + temp11 + temp12 + temp13 + temp14 + temp15 + temp16 + temp17 + temp18 + temp19 + temp20 + temp21 + temp22 + temp23 + temp24 + temp25 + temp26 + temp27 + temp28 + temp29 + temp30 + temp31 + temp32 + temp33 + temp34 + temp35 + temp36 + temp37


        '''计算远mb平均温度'''
        yuan_mb_num = num01 + num02 + num03 + num04 + num05 + num06 + num07 + num08 + num09 + num010 + num011 + num012 + num013 + num014 + num015 + num016 + num017 + num018 + num019 + num020 + num021 + num022 + num023 + num024 + num025 + num026 + num027 + num028 + num029 + num030 + num031 + num032 + num033 + num034 + num035 + num036 + num037
        yuan_mb_temp = temp01 + temp02 + temp03 + num04 + temp05 + temp06 + temp07 + temp08 + temp09 + temp010 + temp011 + temp012 + temp013 + temp014 + temp015 + temp016 + temp017 + temp018 + temp019 + temp020 + temp021 + temp022 + temp023 + temp024 + temp025 + temp026 + num027 + temp028 + temp029 + temp030 + temp031 + temp032 + temp033 + temp034 + temp035 + temp036 + temp037


        ### 计算圆顶内焦面外圈、Mb焦面光阑、远Mb温度传感器、焦面光阑温度传感器的平均温度变化 ###

        '''读取焦面光阑温度 4个'''
        temp9, num9 = re_value(data['106028'][i])
        temp10, num10 = re_value(data['106030'][i])
        temp11, num11 = re_value(data['107000'][i])
        temp12, num12 = re_value(data['107001'][i])

        focal_stop_num = num9 + num10 + num11 + num12
        focal_stop_temp = temp9 + temp10 + temp11 + temp12

        '''读取mb焦面光阑 6 个'''
        temp13, num13 = re_value(data['107010'][i])
        temp14, num14 = re_value(data['107011'][i])
        temp15, num15 = re_value(data['107012'][i])
        temp16, num16 = re_value(data['107013'][i])
        temp17, num17 = re_value(data['107014'][i])
        temp18, num18 = re_value(data['107015'][i])

        mb_stop_num = num13 + num14 + num15 + num16 + num17 + num18
        mb_stop_temp = temp13 + temp14 + temp15 + temp16 + temp17 + temp18
        if jin_mb_num == 0 or yuan_mb_num == 0 or focal_stop_num ==0 or mb_stop_num == 0:
            continue
        else:
            jin_mb_mean = jin_mb_temp / jin_mb_num
            yuan_mb_mean = yuan_mb_temp/yuan_mb_num
            focal_stop_mean = focal_stop_temp / focal_stop_num
            mb_stop_mean = mb_stop_temp/mb_stop_num

            new_data_mbjin.append([data['time'][i],jin_mb_mean])
            new_data_mbyuan.append([data['time'][i],yuan_mb_mean])
            new_data_focalstop.append([data['time'][i],focal_stop_mean])
            new_data_mbstop.append([data['time'][i],mb_stop_mean])

    '''保存Mb数据'''
    name = ['time','temp_mean']
    temp_0 = pd.DataFrame(columns=name,data = new_data_mbjin).set_index(['time'])
    temp_0.index = pd.to_datetime(temp_0.index)
    new_data_mbjin_mean = temp_0.resample('10Min').mean()
    new_data_mbjin_change = temp_0.resample('10Min').apply(custom_resampler)
    new_data_mbjin_change.rename('mbjin_change', inplace=True)
    mbjin_data = pd.merge(new_data_mbjin_mean,new_data_mbjin_change, on='time')
    mbjin_data = mbjin_data.dropna()

    name = ['time','temp_mean']
    temp_1 = pd.DataFrame(columns=name, data=new_data_mbyuan).set_index(['time'])
    temp_1.index = pd.to_datetime(temp_1.index)
    new_data_mbyuan_mean = temp_1.resample('10Min').mean()
    new_data_mbyuan_change = temp_1.resample('10Min').apply(custom_resampler)
    new_data_mbyuan_change.rename('mbyuan_change', inplace=True)
    mbyuan_data = pd.merge(new_data_mbyuan_mean, new_data_mbyuan_change, on='time')
    mbyuan_data = mbyuan_data.dropna()

    '''保存焦面stop数据'''
    name = ['time', 'temp_mean']
    temp_2 = pd.DataFrame(columns=name, data=new_data_focalstop).set_index(['time'])
    temp_2.index = pd.to_datetime(temp_2.index)
    data_focalstop_mean = temp_2.resample('10Min').mean()
    data_focalstop_change = temp_2.resample('10Min').apply(custom_resampler)
    data_focalstop_change.rename('data_focal_change', inplace=True)
    focalstop_data = pd.merge(data_focalstop_mean, data_focalstop_change, on='time')
    focalstop_data = focalstop_data.dropna()

    '''保存mb stop数据'''
    name = ['time', 'temp_mean']
    temp_3 = pd.DataFrame(columns=name, data=new_data_mbstop).set_index(['time'])
    temp_3.index = pd.to_datetime(temp_3.index)
    data_mbstop_mean = temp_3.resample('10Min').mean()
    data_mbstop_change = temp_3.resample('10Min').apply(custom_resampler)
    data_mbstop_change.rename('data_mbstop_change', inplace=True)
    mbstop_data = pd.merge(data_mbstop_mean, data_mbstop_change, on='time')
    mbstop_data = mbstop_data.dropna()

    new_temp_01 = pd.merge(mbjin_data,mbyuan_data, on='time')
    new_temp_02 = pd.merge(focalstop_data,mbstop_data, on='time')
    new_temp16789 = pd.merge(new_temp_01,new_temp_02,on='time')
    return new_temp16789