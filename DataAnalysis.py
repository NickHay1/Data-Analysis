# -*- coding: utf-8 -*-
import pandas as pd

df = pd.read_spss(r'FilePath')

#Create Costs
cost_dict = {}
def gen_data():
    for x in range(1, 9):
        url = r'FilePath'.format(x)
        data = pd.read_spss(url)
        cost_dict[x] = data
gen_data()    

def total_cost(row, df):
    columns = df.columns
    columns = [c for c in columns if 'cst' in c or 'cost' in c]
    cstTotal = 0
    for c in columns: cstTotal += row[c]
    return cstTotal

for (i, ds) in zip(range(1, 9), cost_dict.values()):
    if i == 8: 
        ds['cstTotal'] = 11600
    else:
        ds['cstTotal'] = ds.apply(lambda row: total_cost(row, ds), axis=1)
    ds['Teleno'] = ds['Teleno'].apply(lambda column: column[1:])
    ds = ds.loc[:, ['Teleno', 'cstTotal', 'Scenario']]
    cost_dict[i] = ds
    
df_cost = pd.concat(cost_dict, ignore_index=True)
df = pd.merge(
    left=df, right=df_cost, left_on=['UPRN', 'ScenarioID'], right_on=['Teleno', 'Scenario'], how='left'
)

#Report Tables
df_filter = df.loc[df['SimpleSAP'] >= 69]
df_gn = df_filter.loc[df_filter['cstTotal'] > 0]

count_ = df_filter['batch'].value_counts() #get count
count_.sort_index(inplace=True)
cost = df_gn.groupby(['batch']).mean() #get average cost
cost = cost.sort_values('batch')

table = pd.DataFrame(
    data={
        'Scenario': sorted(list(df['batch'].unique())), 'Count': count_.values, 
        '% of Dwellings': [x/len(ds) for x in count_.values],
        'Cost Per Dwelling': [0] + cost['cstTotal'].tolist(), 
        'Total Cost': [x*y for (x, y) in zip(count_, [0] + cost['cstTotal'].tolist())]
    }
)
table.to_csv(
    r'FilePath', 
    index=False
)
       
"""
Created on Tue Jul 14 12:41:56 2020

@author: hayn
"""


