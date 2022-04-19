# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 07:41:01 2022

Process a text file containing Nexus Clash Logs into a nice dataframe, and generate some basic summaries

@author: ckswi
"""


import pandas as pd


def extract_log_data(log_data, column_names, parse_regex):
    log_data.loc[:, column_names] = log_data.log_line.str.extract(parse_regex).values
    return log_data

parsers = [
    [['damage_dealt','damage_type_dealt'], r'They take (\d+) points of (\w+) damage'],
    [['damage_taken','damage_type_taken'], r'attacked you and hit for (\d+) points of (\w+) damage'],
    [['time_stamp'], r'(\d+-\d+-\d+ \d+:\d+:\d+)'],
    [['pet_type','pet_master'], r'[a\-)]n? ([\w ]+), belonging to ([\w ]+), attacked you'],
    [['repeated_line'], r'\((\d+) times\)'],
    [['attacker'], r'[a\-)]n? ([\w -]+) attacked you'],
    [['weapon','character_damage_taken', 'character_damage_type_taken'], r'attacked you with an? ([\w -]+) and hit for (\d+) points of (\w+) damage'],
    [['xp'], r'(\d+) (?:XP)|(?:Experience)']
    ]

detectors = [
    ['phased', 'as the attack glances harmlessly off of you'],
    ['missed', 'and missed!'],
    ['attacked', 'attacked you'],
    ['hit','and hit'],
    ['blood_ice','You crush the Blood Ice in your hand'],
    ['heal_pot', 'You quaff the potion. As you do so, you feel its magic flow through you, mending flesh and bone']
    ]

def process_log(fn):
        
    with open(fn, 'rb') as f:
        rl = f.readlines()
    rl = [r.decode() for r in rl]
    
    log_data = pd.DataFrame({'log_line':rl})
    
    for parser in parsers:
        log_data = extract_log_data(log_data, parser[0], parser[1])
    
    for detector in detectors:
        log_data[detector[0]] = log_data.log_line.str.contains(detector[1])
    
    
    for col in log_data.columns:
        try:
            log_data[col] = pd.to_numeric(log_data[col])
        except ValueError:
            pass
    
    
    repeated_lines = log_data.loc[log_data.repeated_line.notna(),:]
    repeated_lines.repeated_line = repeated_lines.repeated_line.astype(int)-1
    
    glublins = []
    
    for i, row in repeated_lines.iterrows():
        for i in range(row.repeated_line):
            row_new=row.copy()
            row_new.repeated_line = i
            glublins.append(row_new)
    if len(glublins) > 0:
        new_rows = pd.concat(glublins, axis=1).T
        log_data = pd.concat([log_data, new_rows], axis=0)

    return log_data



def pet_summarise_master_type(log_data):
    pet_summary = log_data.groupby(['pet_master','pet_type','attacked']).agg({'damage_taken':['sum', 'max'], 'attacked':'sum', 'missed':'sum', 'hit':'sum'})
    pet_summary['accuracy'] = pet_summary.hit/pet_summary.attacked
    pet_summary
    return pet_summary
    
def pet_summarise_type(log_data):
    pet_summary_2 = log_data.groupby(['pet_type','attacked']).agg({'damage_taken':['sum', 'max'], 'attacked':'sum', 'missed':'sum', 'hit':'sum'})
    pet_summary_2['accuracy'] = pet_summary_2.hit/pet_summary_2.attacked
    pet_summary_2
    return pet_summary_2
    
    
def char_summarise(log_data):
    dd = log_data.query('attacked').groupby(['attacker']).agg({'character_damage_taken':'sum', 'attacked':'count', 'hit':'sum'})
    return dd
    
    
    
    char_summary = log_data.query('attacked').groupby(['character_damage_type_taken']).agg({'character_damage_taken':['sum','mean','min','max'], 'hit':'count'})
    



if __name__ == '__main__':
    
    import sys
    try:
        fn = sys.argv[1]
    except:
        fn = 'C:/Users/ckswi/Google Drive/Nexus/khaze/2022-04-18_TROvsDND.txt'
    
    log_data = process_log(fn)
    
    pet_summary = pet_summarise_master_type(log_data)
    pet_summary_type = pet_summarise_type(log_data)
    try:
        char_log = char_summarise(log_data)
    except:
        char_log = None
    
    
    
        








