import pandas as pd 
import numpy as np 

path = "C:\\Users\\HP\\Downloads\\"

name = "Stellaps"

sub_lst = [6,7,8,9,10,11]

# print(path + f'{name}_{sub_lst[0]}.csv')
# df_june = pd.read_csv(path + f'{name}_{sub_lst[0]}.csv',encoding='iso-8859-1')
# df_july = pd.read_csv(path + f'{name}_{sub_lst[1]}.csv',encoding='iso-8859-1')
# df_aug = pd.read_csv(path + f'{name}_{sub_lst[2]}.csv',encoding='iso-8859-1')
# df_sep = pd.read_csv(path + f'{name}_{sub_lst[3]}.csv',encoding='iso-8859-1')
# df_oct = pd.read_csv(path + f'{name}_{sub_lst[4]}.csv',encoding='iso-8859-1')
# df_nov = pd.read_csv(path + f'{name}_{sub_lst[5]}.csv',encoding='iso-8859-1')

df_june = pd.read_excel(path + f'{name}_{sub_lst[0]}.xlsx',encoding='iso-8859-1')
df_july = pd.read_excel(path + f'{name}_{sub_lst[1]}.xlsx',encoding='iso-8859-1')
df_aug = pd.read_excel(path + f'{name}_{sub_lst[2]}.xlsx',encoding='iso-8859-1')
df_sep = pd.read_excel(path + f'{name}_{sub_lst[3]}.xlsx',encoding='iso-8859-1')
df_oct = pd.read_excel(path + f'{name}_{sub_lst[4]}.xlsx',encoding='iso-8859-1')
df_nov = pd.read_excel(path + f'{name}_{sub_lst[5]}.xlsx',encoding='iso-8859-1')

frames = [df_june, df_july, df_aug, df_sep, df_oct, df_nov]

df_all = pd.concat(frames)

print(df_all)
