###  进行数据的特征工程  ###
import pandas
import pandas as pd
import math
from two_year_time import clear_temp
from temp_prepro import clear_pretemp

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

'''读取所有两年的数据文件包括2021年5月到2023年6月'''
'''DIMM数据文件地址'''
dimm_0 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM21.5.csv'
dimm_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM21.6.csv'
dimm_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM21.9.csv'
dimm_3 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM21.10.csv'
dimm_4 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM21.11.csv'
dimm_5 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM21.12.csv'
dimm_6 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.1.csv'
dimm_7 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.2.csv'
dimm_8 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.3.csv'
dimm_9 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.4.csv'
dimm_10 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.5.csv'
dimm_11 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.6.csv'
dimm_12 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.9.csv'
dimm_13 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.10.csv'
dimm_14 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.11.csv'
dimm_15 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM22.12.csv'
dimm_16 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM23.1.csv'
dimm_17 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM23.2.csv'
dimm_18 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM23.3.csv'
dimm_19 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM23.4.csv'
dimm_20 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM23.5.csv'
dimm_21 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/DIMM数据/download-DM23.6.csv'

'''CCD数据文件地址'''
ccd00 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202105.csv'
ccd01 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202106.csv'
ccd02 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202109.csv'
ccd03 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202110.csv'
ccd04 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202111.csv'
ccd05 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202112.csv'
ccd06 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202201.csv'
ccd07 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202202.csv'
ccd08 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202203.csv'
ccd09 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202204.csv'
ccd010 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202205.csv'
ccd011 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202206.csv'
ccd012 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202209.csv'
ccd013 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202210.csv'
ccd014 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202211.csv'
ccd015 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202212.csv'
ccd016 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202301.csv'
ccd017 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202302.csv'
ccd018 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202303.csv'
ccd019 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202304.csv'
ccd020 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202305.csv'
ccd021 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd0_202306.csv'

ccd10 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202105.csv'
ccd11 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202106.csv'
ccd12 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202109.csv'
ccd13 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202110.csv'
ccd14 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202111.csv'
ccd15 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202112.csv'
ccd16 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202201.csv'
ccd17 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202202.csv'
ccd18 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202203.csv'
ccd19 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202204.csv'
ccd110 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202205.csv'
ccd111 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202206.csv'
ccd112 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202209.csv'
ccd113 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202210.csv'
ccd114 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202211.csv'
ccd115 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202212.csv'
ccd116 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202301.csv'
ccd117 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202302.csv'
ccd118 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202303.csv'
ccd119 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202304.csv'
ccd120 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202305.csv'
ccd121 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd1_202306.csv'

ccd20 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202105.csv'
ccd21 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202106.csv'
ccd22 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202109.csv'
ccd23 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202110.csv'
ccd24 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202111.csv'
ccd25 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202112.csv'
ccd26 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202201.csv'
ccd27 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202202.csv'
ccd28 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202203.csv'
ccd29 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202204.csv'
ccd210 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202205.csv'
ccd211 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202206.csv'
ccd212 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202209.csv'
ccd213 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202210.csv'
ccd214 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202211.csv'
ccd215 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202212.csv'
ccd216 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202301.csv'
ccd217 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202302.csv'
ccd218 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202303.csv'
ccd219 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202304.csv'
ccd220 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202305.csv'
ccd221 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd2_202306.csv'

ccd30 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202105.csv'
ccd31 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202106.csv'
ccd32 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202109.csv'
ccd33 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202110.csv'
ccd34 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202111.csv'
ccd35 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202112.csv'
ccd36 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202201.csv'
ccd37 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202202.csv'
ccd38 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202203.csv'
ccd39 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202204.csv'
ccd310 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202205.csv'
ccd311 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202206.csv'
ccd312 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202209.csv'
ccd313 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202210.csv'
ccd314 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202211.csv'
ccd315 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202212.csv'
ccd316 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202301.csv'
ccd317 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202302.csv'
ccd318 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202303.csv'
ccd319 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202304.csv'
ccd320 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202305.csv'
ccd321 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ccd/ccd3_202306.csv'

'''读取环境信息'''
evi_0 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-21.5.csv'
evi_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-21.6.csv'
evi_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-21.9.csv'
evi_3 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-21.10.csv'
evi_4 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-21.11.csv'
evi_5 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-21.12.csv'
evi_6 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.1.csv'
evi_7 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.2.csv'
evi_8 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.3.csv'
evi_9 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.4.csv'
evi_10 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.5.csv'
evi_11 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.6.csv'
evi_12 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.9.csv'
evi_13 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.10.csv'
evi_14 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.11.csv'
evi_15 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-22.12.csv'
evi_16 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-23.1.csv'
evi_17 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-23.2.csv'
evi_18 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-23.3.csv'
evi_19 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-23.4.csv'
evi_20 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-23.5.csv'
evi_21 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/气象数据/download-23.6.csv'

'''ao数据地址'''
ao_0 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2105_ao.csv'
ao_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2106_ao.csv'
ao_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2109_ao.csv'
ao_3 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2110_ao.csv'
ao_4 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2111_ao.csv'
ao_5 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2112_ao.csv'
ao_6 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2201_ao.csv'
ao_7 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2202_ao.csv'
ao_8 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2203_ao.csv'
ao_9 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2204_ao.csv'
ao_10 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2205_ao.csv'
ao_11 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2206_ao.csv'
ao_12 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2209_ao.csv'
ao_13 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2210_ao.csv'
ao_14 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2211_ao.csv'
ao_15 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2212_ao.csv'
ao_16 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2301_ao.csv'
ao_17 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2302_ao.csv'
ao_18 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2303_ao.csv'
ao_19 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2304_ao.csv'
ao_20 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2305_ao.csv'
ao_21 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/ao/2306_ao.csv'

'''圆顶内温度分布数据'''
temp_12 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/102-20210512-20230615.csv'
temp_13_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/103-20210512-20220512.csv'
temp_13_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/103-20220512-20230615.csv'
temp_14_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/104-20210512-20220512.csv'
temp_14_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/104-20220512-20230615.csv'
temp_15_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/105-20210512-20220609.csv'
temp_15_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/105-20220609-20230609.csv'
temp_16_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/106-20210512-20220609.csv'
temp_16_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/106-20220609-20230609.csv'
temp_17_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/107-20210512-20220609.csv'
temp_17_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/107-20220609-20230609.csv'
temp_18_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/108-20210512-20220609.csv'
temp_18_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/108-20220609-20230609.csv'
temp_19_1 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/109-20210512-20220609.csv'
temp_19_2 = '/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/LAMOST温度数据/hu/109-20220609-20230609.csv'

'''数据读取及合并'''
'''读取dimm数据'''
dimm0 = pd.read_csv(dimm_0,header=None, names=['time','dimm_seeing'])
dimm1 = pd.read_csv(dimm_1,header=None, names=['time','dimm_seeing'])
dimm2 = pd.read_csv(dimm_2,header=None, names=['time','dimm_seeing'])
dimm3 = pd.read_csv(dimm_3,header=None, names=['time','dimm_seeing'])
dimm4 = pd.read_csv(dimm_4,header=None, names=['time','dimm_seeing'])
dimm5 = pd.read_csv(dimm_5,header=None, names=['time','dimm_seeing'])
dimm6 = pd.read_csv(dimm_6,header=None, names=['time','dimm_seeing'])
dimm7 = pd.read_csv(dimm_7,header=None, names=['time','dimm_seeing'])
dimm8 = pd.read_csv(dimm_8,header=None, names=['time','dimm_seeing'])
dimm9 = pd.read_csv(dimm_9,header=None, names=['time','dimm_seeing'])
dimm10 = pd.read_csv(dimm_10,header=None, names=['time','dimm_seeing'])
dimm11 = pd.read_csv(dimm_11,header=None, names=['time','dimm_seeing'])
dimm12 = pd.read_csv(dimm_12,header=None, names=['time','dimm_seeing'])
dimm13 = pd.read_csv(dimm_13,header=None, names=['time','dimm_seeing'])
dimm14 = pd.read_csv(dimm_14,header=None, names=['time','dimm_seeing'])
dimm15 = pd.read_csv(dimm_15,header=None, names=['time','dimm_seeing'])
dimm16 = pd.read_csv(dimm_16,header=None, names=['time','dimm_seeing'])
dimm17 = pd.read_csv(dimm_17,header=None, names=['time','dimm_seeing'])
dimm18 = pd.read_csv(dimm_18,header=None, names=['time','dimm_seeing'])
dimm19 = pd.read_csv(dimm_19,header=None, names=['time','dimm_seeing'])
dimm20 = pd.read_csv(dimm_20,header=None, names=['time','dimm_seeing'])
dimm21 = pd.read_csv(dimm_21,header=None, names=['time','dimm_seeing'])
dimm_data = pd.concat([dimm0,dimm1,dimm2,dimm3,dimm4,dimm5,dimm6,dimm7,dimm8,dimm9,dimm10,dimm11,dimm12,dimm13,dimm14,dimm15,dimm16,dimm17,dimm18,dimm19,dimm20,dimm21], ignore_index=True)

# '''保存初采样的dimm数据'''
# dimm_data.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/dimm_data.csv', index=False)

'''剔除不正常的dimm数据,正常值范围为0.5-5.4,并进行降采样'''
new_data = []
for i in range(len(dimm_data)):
    data = dimm_data['dimm_seeing'][i]
    if data < 5.4 and data > 0.5:
        time = dimm_data['time'][i]
        new_data.append([time,data])
    else:
        continue
name = ['time','dimm_seeing']
new_data = pd.DataFrame(columns=name, data = new_data).set_index(['time'])
new_data.index = pd.to_datetime(new_data.index)
new_dimm_data = new_data.resample('10Min').mean()
new_dimm_data = new_dimm_data.dropna()

'''ccd数据合并'''
ccd_00 = pd.read_csv(ccd00, header=None, names=['time','seeing'])
ccd_01 = pd.read_csv(ccd01, header=None, names=['time','seeing'])
ccd_02 = pd.read_csv(ccd02, header=None, names=['time','seeing'])
ccd_03 = pd.read_csv(ccd03, header=None, names=['time','seeing'])
ccd_04 = pd.read_csv(ccd04, header=None, names=['time','seeing'])
ccd_05 = pd.read_csv(ccd05, header=None, names=['time','seeing'])
ccd_06 = pd.read_csv(ccd06, header=None, names=['time','seeing'])
ccd_07 = pd.read_csv(ccd07, header=None, names=['time','seeing'])
ccd_08 = pd.read_csv(ccd08, header=None, names=['time','seeing'])
ccd_09 = pd.read_csv(ccd09, header=None, names=['time','seeing'])
ccd_010 = pd.read_csv(ccd010, header=None, names=['time','seeing'])
ccd_011 = pd.read_csv(ccd011, header=None, names=['time','seeing'])
ccd_012 = pd.read_csv(ccd012, header=None, names=['time','seeing'])
ccd_013 = pd.read_csv(ccd013, header=None, names=['time','seeing'])
ccd_014 = pd.read_csv(ccd014, header=None, names=['time','seeing'])
ccd_015 = pd.read_csv(ccd015, header=None, names=['time','seeing'])
ccd_016 = pd.read_csv(ccd016, header=None, names=['time','seeing'])
ccd_017 = pd.read_csv(ccd017, header=None, names=['time','seeing'])
ccd_018 = pd.read_csv(ccd018, header=None, names=['time','seeing'])
ccd_019 = pd.read_csv(ccd019, header=None, names=['time','seeing'])
ccd_020 = pd.read_csv(ccd020, header=None, names=['time','seeing'])
ccd_021 = pd.read_csv(ccd021, header=None, names=['time','seeing'])
ccd0_data = pd.concat([ccd_00,ccd_01,ccd_02,ccd_03,ccd_04,ccd_05,ccd_06,ccd_07,ccd_08,ccd_09,ccd_010,ccd_011,ccd_012,ccd_013,ccd_014,ccd_015,ccd_016,ccd_017,ccd_018,ccd_019,ccd_020,ccd_021], ignore_index=True)

'''保存初采样的ccd0数据'''
ccd0_data.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/ccd0_data.csv', index=False)

ccd_10 = pd.read_csv(ccd10, header=None, names=['time','seeing'])
ccd_11 = pd.read_csv(ccd11, header=None, names=['time','seeing'])
ccd_12 = pd.read_csv(ccd12, header=None, names=['time','seeing'])
ccd_13 = pd.read_csv(ccd13, header=None, names=['time','seeing'])
ccd_14 = pd.read_csv(ccd14, header=None, names=['time','seeing'])
ccd_15 = pd.read_csv(ccd15, header=None, names=['time','seeing'])
ccd_16 = pd.read_csv(ccd16, header=None, names=['time','seeing'])
ccd_17 = pd.read_csv(ccd17, header=None, names=['time','seeing'])
ccd_18 = pd.read_csv(ccd18, header=None, names=['time','seeing'])
ccd_19 = pd.read_csv(ccd19, header=None, names=['time','seeing'])
ccd_110 = pd.read_csv(ccd110, header=None, names=['time','seeing'])
ccd_111 = pd.read_csv(ccd111, header=None, names=['time','seeing'])
ccd_112 = pd.read_csv(ccd112, header=None, names=['time','seeing'])
ccd_113 = pd.read_csv(ccd113, header=None, names=['time','seeing'])
ccd_114 = pd.read_csv(ccd114, header=None, names=['time','seeing'])
ccd_115 = pd.read_csv(ccd115, header=None, names=['time','seeing'])
ccd_116 = pd.read_csv(ccd116, header=None, names=['time','seeing'])
ccd_117 = pd.read_csv(ccd117, header=None, names=['time','seeing'])
ccd_118 = pd.read_csv(ccd118, header=None, names=['time','seeing'])
ccd_119 = pd.read_csv(ccd119, header=None, names=['time','seeing'])
ccd_120 = pd.read_csv(ccd120, header=None, names=['time','seeing'])
ccd_121 = pd.read_csv(ccd121, header=None, names=['time','seeing'])
ccd1_data = pd.concat([ccd_10,ccd_11,ccd_12,ccd_13,ccd_14,ccd_15,ccd_16,ccd_17,ccd_18,ccd_19,ccd_110,ccd_111,ccd_112,ccd_113,ccd_114,ccd_115,ccd_116,ccd_117,ccd_118,ccd_119,ccd_120,ccd_121], ignore_index=True)

'''保存初采样的ccd1数据'''
ccd1_data.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/ccd1_data.csv', index=False)

ccd_20 = pd.read_csv(ccd20, header=None, names=['time','seeing'])
ccd_21 = pd.read_csv(ccd21, header=None, names=['time','seeing'])
ccd_22 = pd.read_csv(ccd22, header=None, names=['time','seeing'])
ccd_23 = pd.read_csv(ccd23, header=None, names=['time','seeing'])
ccd_24 = pd.read_csv(ccd24, header=None, names=['time','seeing'])
ccd_25 = pd.read_csv(ccd25, header=None, names=['time','seeing'])
ccd_26 = pd.read_csv(ccd26, header=None, names=['time','seeing'])
ccd_27 = pd.read_csv(ccd27, header=None, names=['time','seeing'])
ccd_28 = pd.read_csv(ccd28, header=None, names=['time','seeing'])
ccd_29 = pd.read_csv(ccd29, header=None, names=['time','seeing'])
ccd_210 = pd.read_csv(ccd210, header=None, names=['time','seeing'])
ccd_211 = pd.read_csv(ccd211, header=None, names=['time','seeing'])
ccd_212 = pd.read_csv(ccd212, header=None, names=['time','seeing'])
ccd_213 = pd.read_csv(ccd213, header=None, names=['time','seeing'])
ccd_214 = pd.read_csv(ccd214, header=None, names=['time','seeing'])
ccd_215 = pd.read_csv(ccd215, header=None, names=['time','seeing'])
ccd_216 = pd.read_csv(ccd216, header=None, names=['time','seeing'])
ccd_217 = pd.read_csv(ccd217, header=None, names=['time','seeing'])
ccd_218 = pd.read_csv(ccd218, header=None, names=['time','seeing'])
ccd_219 = pd.read_csv(ccd219, header=None, names=['time','seeing'])
ccd_220 = pd.read_csv(ccd220, header=None, names=['time','seeing'])
ccd_221 = pd.read_csv(ccd221, header=None, names=['time','seeing'])
ccd2_data = pd.concat([ccd_20,ccd_21,ccd_22,ccd_23,ccd_24,ccd_25,ccd_26,ccd_27,ccd_28,ccd_29,ccd_210,ccd_211,ccd_212,ccd_213,ccd_214,ccd_215,ccd_216,ccd_217,ccd_218,ccd_219,ccd_220,ccd_221], ignore_index=True)

'''保存初采样的ccd2数据'''
ccd2_data.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/ccd2_data.csv', index=False)

ccd_30 = pd.read_csv(ccd30, header=None, names=['time','seeing'])
ccd_31 = pd.read_csv(ccd31, header=None, names=['time','seeing'])
ccd_32 = pd.read_csv(ccd32, header=None, names=['time','seeing'])
ccd_33 = pd.read_csv(ccd33, header=None, names=['time','seeing'])
ccd_34 = pd.read_csv(ccd34, header=None, names=['time','seeing'])
ccd_35 = pd.read_csv(ccd35, header=None, names=['time','seeing'])
ccd_36 = pd.read_csv(ccd36, header=None, names=['time','seeing'])
ccd_37 = pd.read_csv(ccd37, header=None, names=['time','seeing'])
ccd_38 = pd.read_csv(ccd38, header=None, names=['time','seeing'])
ccd_39 = pd.read_csv(ccd39, header=None, names=['time','seeing'])
ccd_310 = pd.read_csv(ccd310, header=None, names=['time','seeing'])
ccd_311 = pd.read_csv(ccd311, header=None, names=['time','seeing'])
ccd_312 = pd.read_csv(ccd312, header=None, names=['time','seeing'])
ccd_313 = pd.read_csv(ccd313, header=None, names=['time','seeing'])
ccd_314 = pd.read_csv(ccd314, header=None, names=['time','seeing'])
ccd_315 = pd.read_csv(ccd315, header=None, names=['time','seeing'])
ccd_316 = pd.read_csv(ccd316, header=None, names=['time','seeing'])
ccd_317 = pd.read_csv(ccd317, header=None, names=['time','seeing'])
ccd_318 = pd.read_csv(ccd318, header=None, names=['time','seeing'])
ccd_319 = pd.read_csv(ccd319, header=None, names=['time','seeing'])
ccd_320 = pd.read_csv(ccd320, header=None, names=['time','seeing'])
ccd_321 = pd.read_csv(ccd321, header=None, names=['time','seeing'])
ccd3_data = pd.concat([ccd_30,ccd_31,ccd_32,ccd_33,ccd_34,ccd_35,ccd_36,ccd_37,ccd_38,ccd_39,ccd_310,ccd_311,ccd_312,ccd_313,ccd_314,ccd_315,ccd_316,ccd_317,ccd_318,ccd_319,ccd_320,ccd_321], ignore_index=True)

'''保存初采样的ccd1数据'''
ccd3_data.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/ccd3_data.csv', index=False)

'''ccd数据的正常值范围为1.5-6.1,对ccd0-ccd3的数据进行清洗，并取平均合并为一个数据'''

def ccd_data_del(lab0,lab1,data):
    new_data = []
    lab0 = float(lab0)
    lab1 = float(lab1)
    for i in range(len(data)):
        data0 = data['seeing'][i]
        if data0 < lab0 and data0 > lab1:
            time = ccd0_data['time'][i]
            new_data.append([time, data0])
        else:
            continue
    name = ['time', 'seeing']
    new_ccd_data = pd.DataFrame(columns=name, data=new_data).set_index(['time'])
    new_ccd_data.index = pd.to_datetime(new_ccd_data.index)
    return new_ccd_data

new_ccd0 = ccd_data_del(6.1,0.5,ccd0_data)
new_ccd1 = ccd_data_del(6.1,0.5,ccd1_data)
new_ccd2 = ccd_data_del(6.1,0.5,ccd2_data)
new_ccd3 = ccd_data_del(6.1,0.5,ccd3_data)

new_ccd0 = new_ccd0.resample('1min').mean()
new_ccd1 = new_ccd1.resample('1min').mean()
new_ccd2 = new_ccd2.resample('1min').mean()
new_ccd3 = new_ccd3.resample('1min').mean()

data0 = pd.merge(new_ccd0,new_ccd1,on = 'time')
data1 = pd.merge(new_ccd2,new_ccd3,on = 'time')
ccd_data = pd.merge(data0,data1,on = 'time')

'''判断是否为nan,并计算平均值'''
ccd_mean = []
for i in range(len(ccd_data)):
    ccd0 = ccd_data['seeing_x_x'][i]
    ccd1 = ccd_data['seeing_y_x'][i]
    ccd2 = ccd_data['seeing_x_y'][i]
    ccd3 = ccd_data['seeing_y_y'][i]
    if math.isnan(ccd0):
        num0 = 0
        ccd0 = 0
    else:
        num0 = 1
    if math.isnan(ccd1):
        num1 = 0
        ccd1 = 0
    else:
        num1 = 1
    if math.isnan(ccd2):
        num2 = 0
        ccd2 = 0
    else:
        num2 = 1
    if math.isnan(ccd3):
        num3 = 0
        ccd3 = 0
    else:
        num3 = 1
    num = num0 + num1 + num2 + num3
    ccd = ccd0 + ccd1 + ccd2 + ccd3
    if num == 0:
        continue
    else:
        ccd_value = ccd / num
        ccd_mean.append([ccd_data.index[i],ccd_value])

name = ['time','seeing']
new_data = pd.DataFrame(columns=name, data = ccd_mean).set_index(['time'])
new_data.index = pd.to_datetime(new_data.index)
new_ccd_data = new_data.resample('10Min').mean()
new_ccd_data = new_ccd_data.dropna()

'''读取环境数据'''
evi0 = pd.read_csv(evi_0,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi1 = pd.read_csv(evi_1,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi2 = pd.read_csv(evi_2,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi3 = pd.read_csv(evi_3,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi4 = pd.read_csv(evi_4,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi5 = pd.read_csv(evi_5,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi6 = pd.read_csv(evi_6,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi7 = pd.read_csv(evi_7,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi8 = pd.read_csv(evi_8,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi9 = pd.read_csv(evi_9,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi10 = pd.read_csv(evi_10,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi11 = pd.read_csv(evi_11,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi12 = pd.read_csv(evi_12,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi13 = pd.read_csv(evi_13,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi14 = pd.read_csv(evi_14,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi15 = pd.read_csv(evi_15,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi16 = pd.read_csv(evi_16,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi17 = pd.read_csv(evi_17,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi18 = pd.read_csv(evi_18,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi19 = pd.read_csv(evi_19,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi20 = pd.read_csv(evi_20,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi21 = pd.read_csv(evi_21,header=None, names=['time','temperature','humidity','dewpoint','pressure','windspeed_avg','windspeed_max','winddirection'])
evi_data = pd.concat([evi0,evi1,evi2,evi3,evi4,evi5,evi6,evi7,evi8,evi9,evi10,evi11,evi12,evi13,evi14,evi15,evi16,evi17,evi18,evi19,evi20,evi21], ignore_index=True)

'''各项环境数据的正常范围,温度-25-35,平均风速0-12.5,最大风速0-16,气压894-930,露点-40-35,湿度0-125'''
new_data = []
for i in range(len(evi_data)):
    temp = evi_data['temperature'][i]
    hum = evi_data['humidity'][i]
    dew = evi_data['dewpoint'][i]
    pre = evi_data['pressure'][i]
    win_avg = evi_data['windspeed_avg'][i]
    win_max = evi_data['windspeed_max'][i]
    time = evi_data['time'][i]
    if -25 < temp and temp < 35 and hum >= 0 and hum < 125 and dew < 35 and dew > -40 and pre > 894 and pre <930 and win_avg >= 0 and win_avg < 12.5 and win_max >= 0 and win_max < 125:
        new_data.append([time,temp,hum,dew,pre,win_avg,win_max])
    else:
        continue
name = ['time','temp','humidity','dewpoint','pre','wind_avg','wind_max']
new_data = pd.DataFrame(columns=name, data = new_data).set_index(['time'])
new_data.index = pd.to_datetime(new_data.index)
new_evi_data = new_data.resample('10Min').mean()
new_evi_data = new_evi_data.dropna()

'''保存初采样的ccd1数据'''
evi_data.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/evi_data.csv', index=False)

'''读取ao数据'''
ao_optics_0 = pd.read_csv(ao_0,header=None, names=['time','ao_seeing'])
ao_optics_1 = pd.read_csv(ao_1,header=None, names=['time','ao_seeing'])
ao_optics_2 = pd.read_csv(ao_2,header=None, names=['time','ao_seeing'])
ao_optics_3 = pd.read_csv(ao_3,header=None, names=['time','ao_seeing'])
ao_optics_4 = pd.read_csv(ao_4,header=None, names=['time','ao_seeing'])
ao_optics_5 = pd.read_csv(ao_5,header=None, names=['time','ao_seeing'])
ao_optics_6 = pd.read_csv(ao_6,header=None, names=['time','ao_seeing'])
ao_optics_7 = pd.read_csv(ao_7,header=None, names=['time','ao_seeing'])
ao_optics_8 = pd.read_csv(ao_8,header=None, names=['time','ao_seeing'])
ao_optics_9 = pd.read_csv(ao_9,header=None, names=['time','ao_seeing'])
ao_optics_10 = pd.read_csv(ao_10,header=None, names=['time','ao_seeing'])
ao_optics_11 = pd.read_csv(ao_11,header=None, names=['time','ao_seeing'])
ao_optics_12 = pd.read_csv(ao_12,header=None, names=['time','ao_seeing'])
ao_optics_13 = pd.read_csv(ao_13,header=None, names=['time','ao_seeing'])
ao_optics_14 = pd.read_csv(ao_14,header=None, names=['time','ao_seeing'])
ao_optics_15 = pd.read_csv(ao_15,header=None, names=['time','ao_seeing'])
ao_optics_16 = pd.read_csv(ao_16,header=None, names=['time','ao_seeing'])
ao_optics_17 = pd.read_csv(ao_17,header=None, names=['time','ao_seeing'])
ao_optics_18 = pd.read_csv(ao_18,header=None, names=['time','ao_seeing'])
ao_optics_19 = pd.read_csv(ao_19,header=None, names=['time','ao_seeing'])
ao_optics_20 = pd.read_csv(ao_20,header=None, names=['time','ao_seeing'])
ao_optics_21 = pd.read_csv(ao_21,header=None, names=['time','ao_seeing'])
ao_data = pd.concat([ao_optics_0,ao_optics_1,ao_optics_2,ao_optics_3,ao_optics_4,ao_optics_5,ao_optics_6,ao_optics_7,ao_optics_8,ao_optics_9,ao_optics_10,ao_optics_11,ao_optics_12,ao_optics_13,ao_optics_14,ao_optics_15,ao_optics_16,ao_optics_17,ao_optics_18,ao_optics_19,ao_optics_20,ao_optics_21], ignore_index=True)

# '''保存初采样的ao数据'''
# ao_data.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/ao_data.csv', index=False)

'''AO数据的正常范围为0-5'''
new_data = []
for i in range(len(ao_data)):
    ao0 = ao_data['ao_seeing'][i]
    time = ao_data['time'][i]
    if ao0 > 0 and ao0 < 5:
        new_data.append([time,ao0])
    else:
        continue
name = ['time','ao_data']
new_data = pd.DataFrame(columns=name, data = new_data).set_index(['time'])
new_data.index = pd.to_datetime(new_data.index)
new_ao_data = new_data.resample('10Min').mean()
new_ao_data = new_ao_data.dropna()

'''温度数据读取及合并'''
temp12 = pd.read_csv(temp_12,usecols=['id','time','temp'])
temp13_1 = pd.read_csv(temp_13_1,usecols=['id','time','temp'])
temp13_2 = pd.read_csv(temp_13_2,usecols=['id','time','temp'])
temp14_1 = pd.read_csv(temp_14_1,usecols=['id','time','temp'])
temp14_2 = pd.read_csv(temp_14_2,usecols=['id','time','temp'])
temp15_1 = pd.read_csv(temp_15_1,usecols=['id','time','temp'])
temp15_2 = pd.read_csv(temp_15_2,usecols=['id','time','temp'])
temp16_1 = pd.read_csv(temp_16_1,usecols=['id','time','temp'])
temp16_2 = pd.read_csv(temp_16_2,usecols=['id','time','temp'])
temp17_1 = pd.read_csv(temp_17_1,usecols=['id','time','temp'])
temp17_2 = pd.read_csv(temp_17_2,usecols=['id','time','temp'])
temp18_1 = pd.read_csv(temp_18_1,usecols=['id','time','temp'])
temp18_2 = pd.read_csv(temp_18_2,usecols=['id','time','temp'])
temp19_1 = pd.read_csv(temp_19_1,usecols=['id','time','temp'])
temp19_2 = pd.read_csv(temp_19_2,usecols=['id','time','temp'])
temp13 = pd.concat([temp13_1,temp13_2],ignore_index=True)
temp14 = pd.concat([temp14_1,temp14_2],ignore_index=True)
temp15 = pd.concat([temp15_1,temp15_2],ignore_index=True)
temp16 = pd.concat([temp16_1,temp16_2],ignore_index=True)
temp17 = pd.concat([temp17_1,temp17_2],ignore_index=True)
temp18 = pd.concat([temp18_1,temp18_2],ignore_index=True)
temp19 = pd.concat([temp19_1,temp19_2],ignore_index=True)

# '''保存初采样的w温度数据'''
# temp12.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp12.csv', index=False)
# temp13.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp13.csv', index=False)
# temp14.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp14.csv', index=False)
# temp15.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp15.csv', index=False)
# temp16.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp16.csv', index=False)
# temp17.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp17.csv', index=False)
# temp18.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp18.csv', index=False)
# temp19.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp19.csv', index=False)

'''温度数据清洗，正常值范围为-25-40，对数据进行降采样'''
'''对Ma_12温度数据进行处理'''

new_data = []
for i in range(len(temp12)-1):
    if temp12['id'][i] == 102000 and temp12['id'][i+15] == 102015:
        data1, num1 = re_value(temp12['temp'][i])
        data2, num2 = re_value(temp12['temp'][i+1])
        data3, num3 = re_value(temp12['temp'][i+2])
        data4, num4 = re_value(temp12['temp'][i+3])
        data5, num5 = re_value(temp12['temp'][i+4])
        data6, num6 = re_value(temp12['temp'][i+5])
        data7, num7 = re_value(temp12['temp'][i+6])
        data8, num8 = re_value(temp12['temp'][i+7])
        data9, num9 = re_value(temp12['temp'][i+8])
        data10, num10 = re_value(temp12['temp'][i+9])
        data11, num11 = re_value(temp12['temp'][i+10])
        data12, num12 = re_value(temp12['temp'][i+11])
        data13, num13 = re_value(temp12['temp'][i+12])
        data14, num14 = re_value(temp12['temp'][i+13])
        data15, num15 = re_value(temp12['temp'][i+14])
        data16, num16 = re_value(temp12['temp'][i+15])

        temp_value_sum = data1 + data2 + data3 + data4 + data5 + data6 + data7 + data8 + data9 + data10 + data11 + data12 + data13 + data14 + data15 + data16
        temp_num = num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12 + num13 + num14 + num15 + num16
        if temp_num == 0:
            continue
        else:
            temp_mean = temp_value_sum/temp_num
            new_data.append([temp12['time'][i],temp_mean])
    else:
        continue
name = ['time','temp_mean']
temp = pd.DataFrame(columns=name, data = new_data).set_index(['time'])
temp.index = pd.to_datetime(temp.index)
temp12_mean = temp.resample('10Min').mean()
temp12_change = temp.resample('10Min').apply(custom_resampler)
temp12_change.rename('temp12_change',inplace=True)
new_temp12 = pandas.merge(temp12_mean,temp12_change,on = 'time')
new_temp12 = new_temp12.dropna()

'''对ma_13温度数据进行清洗和降采样'''
new_data = []
for i in range(len(temp13)-1):
    if temp13['id'][i] == 103000 and temp13['id'][i+28] == 103028:
        data1, num1 = re_value(temp13['temp'][i])
        data2, num2 = re_value(temp13['temp'][i+1])
        data3, num3 = re_value(temp13['temp'][i+2])
        data4, num4 = re_value(temp13['temp'][i+3])
        data5, num5 = re_value(temp13['temp'][i+4])
        data6, num6 = re_value(temp13['temp'][i+5])
        data7, num7 = re_value(temp13['temp'][i+6])
        data8, num8 = re_value(temp13['temp'][i+7])
        data9, num9 = re_value(temp13['temp'][i+8])
        data10, num10 = re_value(temp13['temp'][i+9])
        data11, num11 = re_value(temp13['temp'][i+10])
        data12, num12 = re_value(temp13['temp'][i+11])
        data13, num13 = re_value(temp13['temp'][i+12])
        data14, num14 = re_value(temp13['temp'][i+13])
        data15, num15 = re_value(temp13['temp'][i+14])
        data16, num16 = re_value(temp13['temp'][i+15])
        data17, num17 = re_value(temp13['temp'][i+16])
        data18, num18 = re_value(temp13['temp'][i+17])
        data19, num19 = re_value(temp13['temp'][i+18])
        data20, num20 = re_value(temp13['temp'][i+19])
        data21, num21 = re_value(temp13['temp'][i+20])
        data22, num22 = re_value(temp13['temp'][i+21])
        data23, num23 = re_value(temp13['temp'][i+22])
        data24, num24 = re_value(temp13['temp'][i+23])
        data25, num25 = re_value(temp13['temp'][i+24])
        data26, num26 = re_value(temp13['temp'][i+25])
        data27, num27 = re_value(temp13['temp'][i+26])
        data28, num28 = re_value(temp13['temp'][i+27])
        data29, num29 = re_value(temp13['temp'][i+28])
        temp_value_sum = data1 + data2 + data3 + data4 + data5 + data6 + data7 + data8 + data9 + data10 + data11 + data12 + data13 + data14 + data15 + data16 + data17 + data18 + data19 + data20 + data21 + data22 + data23 + data24 + data25 + data26 + data27 + data28 + data29
        temp_num = num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18 + num19 + num20 + num21 + num22 + num23 + num24 + num25 + num26 + num27 + num28 + num29
        if temp_num == 0:
            continue
        else:
            temp_mean = temp_value_sum/temp_num
            new_data.append([temp13['time'][i],temp_mean])
    else:
        continue
name = ['time','temp_mean']
temp = pd.DataFrame(columns=name, data = new_data).set_index(['time'])
temp.index = pd.to_datetime(temp.index)
temp13_mean = temp.resample('10Min').mean()
temp13_change = temp.resample('10Min').apply(custom_resampler)
temp13_change.rename('temp13_change',inplace=True)
new_temp13 = pandas.merge(temp13_mean,temp13_change,on = 'time')
new_temp13 = new_temp13.dropna()

'''对ma_14的第三层和第四层温度数据进行清洗和降采样'''
new_data_3 = []
new_data_4 = []
for i in range(len(temp14)-1):
    if temp14['id'][i] == 104000 and temp14['id'][i+28] == 104028:
        data1, num1 = re_value(temp14['temp'][i])
        data2, num2 = re_value(temp14['temp'][i+1])
        data3, num3 = re_value(temp14['temp'][i+2])
        data4, num4 = re_value(temp14['temp'][i+3])
        data5, num5 = re_value(temp14['temp'][i+4])
        data6, num6 = re_value(temp14['temp'][i+5])
        data7, num7 = re_value(temp14['temp'][i+6])
        data8, num8 = re_value(temp14['temp'][i+7])
        data9, num9 = re_value(temp14['temp'][i+8])
        data10, num10 = re_value(temp14['temp'][i+9])
        data11, num11 = re_value(temp14['temp'][i+10])
        data12, num12 = re_value(temp14['temp'][i+11])
        data13, num13 = re_value(temp14['temp'][i+12])
        data14, num14 = re_value(temp14['temp'][i+13])
        data15, num15 = re_value(temp14['temp'][i+14])
        data16, num16 = re_value(temp14['temp'][i+15])
        data17, num17 = re_value(temp14['temp'][i+16])
        data18, num18 = re_value(temp14['temp'][i+17])
        data19, num19 = re_value(temp14['temp'][i+18])
        data20, num20 = re_value(temp14['temp'][i+19])
        data21, num21 = re_value(temp14['temp'][i+20])
        data22, num22 = re_value(temp14['temp'][i+21])
        data23, num23 = re_value(temp14['temp'][i+22])
        data24, num24 = re_value(temp14['temp'][i+23])
        data25, num25 = re_value(temp14['temp'][i+24])
        data26, num26 = re_value(temp14['temp'][i+25])
        data27, num27 = re_value(temp14['temp'][i+26])
        data28, num28 = re_value(temp14['temp'][i+27])
        data29, num29 = re_value(temp14['temp'][i+28])
        temp_value_sum_3 = data1 + data2 + data3 + data4 + data5 + data6 + data7 + data8 + data9 + data10 + data11 + data12 + data13 + data14 + data15 + data16 + data17 + data18
        temp_num_3 = num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18
        temp_value_sum_4 = data19 + data20 + data21 + data22 + data23 + data24 + data25 + data26 + data27 + data28 + data29
        temp_num_4 = num19 + num20 + num21 + num22 + num23 + num24 + num25 + num26 + num27 + num28 + num29
        if temp_num_3 == 0:
            continue
        else:
            temp_mean_3 = temp_value_sum_3/temp_num_3
            new_data_3.append([temp14['time'][i],temp_mean_3])
        if temp_num_4 == 0:
            continue
        else:
            temp_mean_4 = temp_value_sum_4/temp_num_4
            new_data_4.append([temp14['time'][i],temp_mean_4])
    else:
        continue
name = ['time','temp_mean']
temp = pd.DataFrame(columns=name, data = new_data_3).set_index(['time'])
temp.index = pd.to_datetime(temp.index)
temp14_3_mean = temp.resample('10Min').mean()
temp14_3_change = temp.resample('10Min').apply(custom_resampler)
temp14_3_change.rename('temp14_3_change',inplace=True)
new_temp14_3 = pandas.merge(temp14_3_mean,temp14_3_change,on = 'time')
new_temp14_3 = new_temp14_3.dropna()

name = ['time','temp_mean']
temp = pd.DataFrame(columns=name, data = new_data_4).set_index(['time'])
temp.index = pd.to_datetime(temp.index)
temp14_4_mean = temp.resample('10Min').mean()
temp14_4_change = temp.resample('10Min').apply(custom_resampler)
temp14_4_change.rename('temp14_4_change',inplace=True)
new_temp14_4 = pandas.merge(temp14_4_mean,temp14_4_change,on = 'time')
new_temp14_4 = new_temp14_4.dropna()

'''对焦面附近，近焦面板和远焦面板的两部分温度进行数据清洗和降采样'''

'''对ma_14的第三层和第四层温度数据进行清洗和降采样'''
new_data_in = []
new_data_out = []
for i in range(len(temp15)-1):
    if temp15['id'][i] == 105000 and temp15['id'][i+19] == 105019:
        data1, num1 = re_value(temp15['temp'][i])
        data2, num2 = re_value(temp15['temp'][i+1])
        data3, num3 = re_value(temp15['temp'][i+2])
        data4, num4 = re_value(temp15['temp'][i+3])
        data5, num5 = re_value(temp15['temp'][i+4])
        data6, num6 = re_value(temp15['temp'][i+5])
        data7, num7 = re_value(temp15['temp'][i+6])
        data8, num8 = re_value(temp15['temp'][i+7])
        data9, num9 = re_value(temp15['temp'][i+8])
        data10, num10 = re_value(temp15['temp'][i+9])
        data11, num11 = re_value(temp15['temp'][i+10])
        data12, num12 = re_value(temp15['temp'][i+11])
        data13, num13 = re_value(temp15['temp'][i+12])
        data14, num14 = re_value(temp15['temp'][i+13])
        data15, num15 = re_value(temp15['temp'][i+14])
        data16, num16 = re_value(temp15['temp'][i+15])
        data17, num17 = re_value(temp15['temp'][i+16])
        data18, num18 = re_value(temp15['temp'][i+17])
        data19, num19 = re_value(temp15['temp'][i+18])
        data20, num20 = re_value(temp15['temp'][i+19])

        temp_value_sum_in = data1 + data2 + data3 + data4 + data5 + data6 + data7 + data8 + data9 + data10 + data11 + data12
        temp_num_in = num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12
        temp_value_sum_out = data13 + data14 + data15 + data16 + data17 + data18 + data19 + data20
        temp_num_out = num13 + num14 + num15 + num16 + num17 + num18 + num19 + num20
        if temp_num_in == 0:
            continue
        else:
            temp_mean_in = temp_value_sum_in/temp_num_in
            new_data_in.append([temp15['time'][i],temp_mean_in])
        if temp_num_out == 0:
            continue
        else:
            temp_mean_out = temp_value_sum_out/temp_num_out
            new_data_out.append([temp15['time'][i],temp_mean_out])
    else:
        continue
name = ['time','temp_mean']
temp = pd.DataFrame(columns=name, data = new_data_in).set_index(['time'])
temp.index = pd.to_datetime(temp.index)
temp15_in_mean = temp.resample('10Min').mean()
temp15_in_change = temp.resample('10Min').apply(custom_resampler)
temp15_in_change.rename('temp15_in_change',inplace=True)
new_temp15_in = pandas.merge(temp15_in_mean,temp15_in_change,on = 'time')
new_temp15_in = new_temp15_in.dropna()

name = ['time','temp_mean']
temp = pd.DataFrame(columns=name, data = new_data_out).set_index(['time'])
temp.index = pd.to_datetime(temp.index)
temp15_out_mean = temp.resample('10Min').mean()
temp15_out_change = temp.resample('10Min').apply(custom_resampler)
temp15_out_change.rename('temp15_out_change',inplace=True)
new_temp15_out = pandas.merge(temp15_out_mean,temp15_out_change,on = 'time')
new_temp15_out = new_temp15_out.dropna()

'''对Mb、Mb光阑及焦面光阑的温度进行数据清洗并降采样'''
'''调用温度函数进行计算'''
'''先合并temp16、temp17、temp18、temp19四个文件'''
temp_16789 = clear_temp(temp16,temp17,temp18,temp19)
temp_16789.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp_16789.csv',index=True)
temp_16789 = pd.read_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/temp_16789.csv')
new_temp_16789 = clear_pretemp(temp_16789)

'''
将所有数据进行合并new_temp_16789、new_temp15_out、new_temp15_in、new_temp14_4、new_temp14_3、new_temp13、
new_temp12、new_evi_data、new_ao_data、new_ccd_data、new_dimm_data
'''

new0 = pd.merge(new_ccd_data, new_dimm_data, on='time')
new1 = pd.merge(new_temp12,new_evi_data, on='time')
new2 = pd.merge(new_temp14_3,new_temp13, on='time')
new3 = pd.merge(new_temp15_in,new_temp14_4,on='time')
new4 = pd.merge(new_temp_16789,new_temp15_out,on='time')

new01 = pd.merge(new0,new1,on='time')
new23 = pd.merge(new2,new3,on='time')
new0123 = pd.merge(new01,new23,on='time')
new01234 = pd.merge(new0123,new4,on='time')
new = pd.merge(new01234,new_ao_data,on='time')
new.to_csv('/Users/hutianzhu/Downloads/江苏省青年基金BK20221156（已立项）/fresh_data/first_final.csv',index=True)