import sqlite3
import pandas as pd
from collections import Counter
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# getting the PDEP corpus, as in format_pdep.py
conn = sqlite3.connect('../data/pdep/SQL/prepcorp.sqlite')
conn.row_factory = dict_factory
crows = list(conn.execute('SELECT * FROM prepcorp'))

# getting the PDEP definitions
#conn = sqlite3.connect('data/pdep/SQL/prepdefs.sqlite')
conn = sqlite3.connect('../data/pdep/SQL/prepdefs.sqlite')
conn.row_factory = dict_factory
drows = list(conn.execute('SELECT * FROM prepdefs'))

WHITELIST = ['about', 'above', 'across', 'after', 'against', 'among', 'around', 'as', 'at', 'before', 'behind', 'below', 'beneath', 'beside', 'besides', 'between', 'beyond', 'circa', 'despite', 'during', 'except', 'for', 'from', 'in', 'including', 'inside', 'into', 'near', 'of', 'off', 'on', 'onto', 'over', 'per', 'since', 'than', 'through', 'to', 'toward', 'towards', 'under', 'until', 'unto', 'up', 'upon', 'via', 'with', 'without']
PDEPLIST = ["'cept", "'gainst", "'mongst", "'pon", 'a cut above', 'a la', 'abaft', 'aboard', 'about', 'above', 'absent', 'according to', 'across', 'afore', 'after', 'after the fashion of', 'against', 'agin', 'ahead of', 'all for', 'all over', 'along', 'along with', 'alongside', 'amid', 'amidst', 'among', 'anent', 'anti', 'apart from', 'apropos', 'around', 'as', 'as far as', 'as for', 'as from', 'as of', 'as regards', 'as to', 'as well as', 'aside from', 'aslant', 'astraddle', 'astride', 'at', 'at a range of', 'at the hand of', 'at the hands of', 'at the heels of', 'athwart', 'atop', 'back of', 'bar', 'bare of', 'barring', 'because of', 'before', 'behind', 'below', 'beneath', 'beside', 'besides', 'between', 'betwixt', 'beyond', 'but', 'but for', 'by', 'by courtesy of', 'by dint of', 'by force of', 'by means of', 'by reason of', 'by the hand of', 'by the hands of', 'by the name of', 'by virtue of', 'by way of', 'care of', 'chez', 'circa', 'come', 'complete with', 'concerning', 'considering', 'contrary to', 'counting', 'courtesy of', 'cum', 'dehors', 'depending on', 'despite', 'down', 'due to', 'during', 'ere', 'ex', 'except', 'except for', 'excepting', 'excluding', 'exclusive of', 'failing', 'following', 'for', 'for all', 'for the benefit of', 'forbye', 'fore', 'fornent', 'frae', 'from', 'give or take', 'given', 'gone', 'having regard to', 'in', 'in accord with', 'in addition to', 'in advance of', 'in aid of', 'in back of', 'in bed with', 'in behalf of', 'in case of', 'in common with', 'in company with', 'in connection with', 'in consideration of', 'in contravention of', 'in default of', 'in excess of', 'in face of', 'in favor of', 'in favour of', 'in front of', 'in honor of', 'in honour of', 'in keeping with', 'in lieu of', 'in light of', 'in line with', 'in memoriam', 'in need of', 'in peril of', 'in place of', 'in proportion to', 'in re', 'in reference to', 'in regard to', 'in relation to', 'in respect of', 'in sight of', 'in spite of', 'in terms of', 'in the course of', 'in the face of', 'in the fashion of', 'in the grip of', 'in the light of', 'in the matter of', 'in the midst of', 'in the name of', 'in the pay of', 'in the person of', 'in the shape of', 'in the teeth of', 'in the throes of', 'in token of', 'in view of', 'in virtue of', 'including', 'inclusive of', 'inside', 'inside of', 'instead of', 'into', 'irrespective of', 'less', 'like', 'little short of', 'mid', 'midst', 'minus', 'mod', 'modulo', 'more like', 'near', 'near to', 'neath', 'next door to', 'next to', 'nigh', 'nothing short of', 'notwithstanding', "o'", "o'er", 'of', 'of the name of', 'of the order of', 'off', 'on', 'on a level with', 'on a par with', 'on account of', 'on behalf of', 'on pain of', 'on the order of', 'on the part of', 'on the point of', 'on the score of', 'on the strength of', 'on the stroke of', 'on top of', 'onto', 'opposite', 'other than', 'out of', 'out of keeping with', 'out of line with', 'outboard of', 'outside', 'outside of', 'outta', 'outwith', 'over', 'over against', 'over and above', 'overtop', 'owing to', 'pace', 'past', 'pending', 'per', 'plus', 'preparatory to', 'previous to', 'prior to', 'pro', 'pursuant to', 'qua', 're', 'regarding', 'regardless of', 'relative to', 'respecting', 'round', 'round about', 'sans', 'save', 'saving', 'short for', 'short of', 'since', 'subsequent to', 'than', 'thanks to', 'this side of', "thro'", 'through', 'throughout', 'thru', 'thwart', 'till', 'to', 'to the accompaniment of', 'to the tune of', 'together with', 'touching', 'toward', 'towards', 'under', 'under cover of', 'under pain of', 'under sentence of', 'under the heel of', 'underneath', 'unlike', 'until', 'unto', 'up', 'up against', 'up and down', 'up before', 'up for', 'up to', 'upon', 'upside', 'upward of', 'upwards of', 'versus', 'via', 'vice', 'vis-a-vis', 'while', 'with', 'with reference to', 'with regard to', 'with respect to', 'with the exception of', 'within', 'within sight of', 'without']

def pdepinsts(crows):
    # identifying instances for non PDEP prepositions
    count = 0
    nonprep = 0
    nonpreps = set()
    for num_rows_visited, row in enumerate(crows):
        if row['prep'] in PDEPLIST:
            count += 1
        if row['prep'] not in PDEPLIST:
            nonprep += 1
            nonpreps.add(row['prep'])

    print("Instances: " + str(len(crows)), ";", 
          "PDEP: " + str(count), ";",
          "Not Used: " + str(nonprep))
    print("Not Used: " + str(nonpreps))

def unused(crows):
    # counts for the three corpora, primarily identifying instances that are not used
    count = 0
    odd = Counter() # non-prep, adverbs, phrasal verbs
    mwe = Counter() # multiword preposition instances
    non = Counter() # single-word prepositions not used
    off = Counter() # instances with offset character locations
    uncommon = set() # list of single-word prepositions not used
    for num_rows_visited, row in enumerate(crows):
        sense = row['sense']
        source = row['source']
        # Skip odd-looking senses
        if sense in ['unk', 'x', 'pv', 'adverb', '1(!)', '']:
            odd[source] += 1
            continue
        # Skip multiword prepositions like "out of"
        if ' ' in row['prep']:
            mwe[source] += 1
            continue
        if row['inst'] in [577203,]:
            odd[source] += 1
            continue
        # Only keep preps that are relatively common
        if row['prep'].lower() not in WHITELIST:
            non[source] += 1
            uncommon.add(row['prep'])
            continue
        # Index is char-based, so tokenization procedure needs to proceed by first excising the preposition
        prep_offset = row['preploc']
        prep_end = row['sentence'].find(' ', prep_offset+1)
        prep_token = row['sentence'][prep_offset:prep_end].strip()
        # Skip if we don't have a match (some offsets are unreliable)
        if prep_token.lower() != row['prep'].lower():
            off[source] += 1
            continue
        count += 1
    print(count)
    print("Odd: ", odd)
    print("MWE: ", mwe)
    print("Off: ", off)
    print("Non: ", non)
    print("Uncommon: ", uncommon)

def defs(drows):
    # identifying PDEP senses
    count = 0
    defs = set()
    for num_rows_visited, row in enumerate(drows):
        if(num_rows_visited == 1040):
            break
        if(row['prep'] == "apropos of"):
            continue
        sense = row['prep'] + "_" + row['sense']
        defs.add(sense)
        count += 1
    print("PDEP senses: " , str(count))
    return defs

#tsv = "cache\clres_stats\pdep_senses.tsv"
tsv = "..\cache\clres_stats\pdep_senses.tsv"

def with_insts():
    # Read the 'tsv' file into 'insts', iterate through it to create the set
    #   to identify the senses that have instances in PDEP
    used = set()
    senses = pd.read_csv(tsv, sep="\t")
    for _, row in senses.iterrows():
        used.add(row["'sense'"])
    return used

def no_insts(defs,used):
    # identifying PDEP senses that have no corpus instances
    unused = set()
    for i in defs:
        if i in used:
            continue
        unused.add(i)
    return unused

def polyused(used):
    # identifying PDEP senses that have a space in the sense 
    #   also indicating that the tagged instances were viewed
    #   as polysemous
    poly = set()
    for i in used:
        sno = i[i.index("_")+1:]
        if " " in sno:
            poly.add(i)
    return poly

# tributary prepositions in PDEP
trib = ["'cept", "'gainst", "'mongst", "'pon", 'afore', 'agin', 'amidst', 'betwixt', 'fore', 'frae', 'neath', 'nigh', "o'", "o'er", 'outta', 'outwith', 'sans', "thro'", 'thru', 'thwart', 'till', 'toward', 'upon', 'while']

def unused_defs(unused,trib):
    # identify which unused definitions have no instances: those with
    #   tributary prepositions and those that have regular prepositions
    #   that have senses with no instances in any of the corpora
    tribdefs = Counter()
    pdepdefs = Counter()

    for i in unused:
        prep = i[:i.index("_")]
        #print(prep)
        if prep in trib:
            tribdefs[prep] += 1
        else:
            pdepdefs[prep] += 1
    return tribdefs, pdepdefs
    
# adds the sense numbers, appending to the preposition
# identifying instances with polysemous tags
senses = Counter()
poly = Counter()
for num_rows_visited, row in enumerate(rows):
    prep = row['prep'].lower()
    if row['sense'] in ['unk', 'x', 'pv', 'adverb', '1(!)', '']:
        continue
    if row['inst'] in [577203,]:
        continue
    sense = prep + '_' + row['sense']
    senses[sense] += 1
    if ' ' in row['sense']:
        poly[sense] += 1

#tsv = "cache\clres_stats\pdep_senses.tsv"
tsv = "..\cache\clres_stats\pdep_senses.tsv"
#senses = sorted(senses)

"""
newsenses = Counter()
for key in sorted(senses):
    newsenses[key] = senses[key]

with open(tsv,'w') as f:
    for i in newsenses:
        f.write(f"{i}\t{newsenses[i]}\n")
"""
"""
    with open(paths.freq_tsv_path(directory, split, "label"), "w", encoding="utf-8") as f:
        for item, freq in sorted(labels.items(), key=lambda x: -x[1]):
            f.write(f"{item}\t{freq}\n")
"""

#conn = sqlite3.connect('data/pdep/SQL/prepdefs.sqlite')
conn = sqlite3.connect('../data/pdep/SQL/prepdefs.sqlite')
conn.row_factory = dict_factory
rows = list(conn.execute('SELECT * FROM prepdefs'))

count = 0
defs = set()
for num_rows_visited, row in enumerate(rows):
    if(num_rows_visited == 1040):
        break
    if(row['prep'] == "apropos of"):
        continue
    sense = row['prep'] + "_" + row['sense']
    defs.add(sense)
    count += 1

#"""
# Read the 'tsv' file into 'insts', iterate through it to create the set
#   to identify the senses that have instances in PDEP
import pandas as pd
used = set()
insts = pd.read_csv(tsv, sep="\t")
for _, row in insts.iterrows():
    used.add(row['sense'])
#"""

#"""
#
unused = set()
for i in defs:
    if i in used:
        continue
    unused.add(i)
#"""

#"""
# idenifies 'label' not in the PDEP defs
nondefs = set()
for i in senses:
    if(i not in defs):
        nondefs.add(i)
#nondefs = {'from_4(3) 10(7)', 'at_9(5) 11(6)-1', 'on_8(3) 11(5)', 'through_3(1b) 10(3)', 'at_1(1) 4(2b)', 
#           'onto_3(3)', 'through_1(1) 3(1b)', 'with_2(2) 3(2a)', 'into_1(1) 3(3)', 'with_11(7b) 7(5)', 
#           'through_3(1b) 5(1d)'}
#"""

#"""
#
mwe_ents = set()
mwe_sens = set()
for i in defs:
    if " " in i:
        mwe_sens.add(i)
        mwe_ents.add(i[:i.index("_")])
#"""

#"""
#
uwds = set()
usen = set()
ucnt = Counter()
udefs = 0
for i in defs:
    if " " in i:
        continue
    if i not in used:
        continue
    prep = i[:i.index("_")]
    if prep not in uncommon:
        continue
    uwds.add(prep)
    usen.add(i)
    ucnt[prep] += 1
    udefs += 1
#"""

wldefs = {'about_4(3)', 'about_6(n)', 'across_1(1)-1', 'after_10(5a)', 'as_4(n)', 'at_11(6)', 'before_4(3)', 'below_5(n)', 'besides_2(n)', 'from_5(3a)', 'in_10(7a)', 'in_12(9)', 'inside_4(1c)', 'into_9(9)', 'near_5(4)', 'of_1(1)', 'of_18(9)', 'of_8(4)', 'off_7(4)', 'off_8(n)', 'on_21(11)', 'on_6(1e)', 'onto_4(n)', 'onto_9(n)', 'over_15(6)-1', 'over_5(2a)', 'over_8(2d)', 'per_2(2)', 'per_3(3)', 'per_4(n)', 'through_6(1e)', 'to_11(4b)', 'to_12(4c)', 'to_16(8)', 'to_4(1c)', 'to_7(2b)', 'toward_5(3)', 'toward_6(1)', 'under_13(4e)', 'under_16(5b)', 'under_6(2c)', 'unto_2(2)', 'unto_5(n)', 'unto_6(n)', 'unto_7(n)', 'up_4(3)', 'upon_10(4)', 'upon_12(6)', 'upon_13(6a)', 'upon_14(7)', 'upon_15(7a)', 'upon_16(7b)', 'upon_17(8)', 'upon_19(9)', 'upon_20(10)', 'upon_21(11)', 'upon_22(12)', 'upon_23(1)', 'upon_3(1b)', 'upon_4(1c)', 'upon_5(1d)', 'upon_6(1e)', 'upon_7(2)', 'upon_9(3a)-1', 'with_14(8a)', 'with_15(9)', 'with_16(10)', 'without_4(3)'}
indf = {'about_1(1)', 'about_2(1a)', 'about_3(2)', 'about_3(2)-1', 'about_5(3a)', 'above_1(1)', 'above_10(n)', 'above_2(1a)', 'above_3(1b)', 'above_4(2)', 'above_5(2a)', 'above_6(2b)', 'above_7(2c)', 'above_8(2d)', 'above_9(3)', 'across_1(1)', 'across_2(2)', 'across_3(n)', 'after_1(1)', 'after_1(1)-1', 'after_11(n)', 'after_2(1a)', 'after_3(1b)', 'after_4(1c)', 'after_5(2)', 'after_6(2a)', 'after_7(3)', 'after_8(4)', 'after_9(5)', 'against_1(1)', 'against_10(4)', 'against_11(n)', 'against_2(1a)', 'against_3(1b)', 'against_4(2)', 'against_5(2a)', 'against_6(2b)', 'against_7(2c)', 'against_8(3)', 'against_9(3a)', 'among_1(1)', 'among_2(2)', 'among_3(3)', 'among_4(4)', 'around_1(1)', 'around_2(1a)', 'around_3(2)', 'around_4(3)', 'around_4(3)-1', 'around_5(4)', 'around_6(n)', 'as_1(1)', 'as_2(2)', 'as_3(n)', 'at_1(1)', 'at_1(1) 4(2b)', 'at_10(5a)', 'at_11(6)-1', 'at_12(n)', 'at_2(2)', 'at_3(2a)', 'at_4(2b)', 'at_5(3)', 'at_6(3a)', 'at_7(4)', 'at_8(4a)', 'at_9(5)', 'at_9(5) 11(6)-1', 'before_1(1)', 'before_2(2)', 'before_3(2a)', 'behind_1(1)', 'behind_2(1a)', 'behind_3(2)', 'behind_4(2a)', 'behind_5(3)', 'behind_6(3a)', 'behind_7(4)', 'behind_8(5)', 'behind_9(6)', 'below_1(1)', 'below_2(1a)', 'below_3(1b)', 'below_4(2)', 'beneath_1(1)', 'beneath_2(1a)', 'beneath_3(2)', 'beneath_4(2a)', 'beneath_5(2b)', 'beneath_6(2c)', 'beside_1(1)', 'beside_2(1a)', 'beside_3(2)', 'besides_1(1)', 'between_1(1)', 'between_2(2)', 'between_3(3)', 'between_4(4)', 'between_5(4a)', 'between_6(4b)', 'between_7(4c)', 'between_8(5)', 'between_9(5a)', 'beyond_1(1)', 'beyond_2(1a)', 'beyond_3(1b)', 'beyond_4(2)', 'beyond_5(3)', 'beyond_6(3a)', 'beyond_7(4)', 'beyond_8(4a)', 'beyond_9(5)', 'circa_1(1)', 'despite_1(1)', 'during_1(1)', 'during_2(1a)', 'except_1(1)', 'for_1(1)', 'for_10(8a)', 'for_11(9)', 'for_12(10)', 'for_13(11)', 'for_14(12)', 'for_2(2)', 'for_2(2)-1', 'for_3(3)', 'for_4(3a)', 'for_5(4)', 'for_6(5)', 'for_7(6)', 'for_8(7)', 'for_9(8)', 'from_1(1)', 'from_10(7)', 'from_10(7)-1', 'from_11(8)', 'from_12(9)', 'from_12(9)-1', 'from_13(10)', 'from_14(11)', 'from_2(1a)', 'from_3(2)', 'from_4(3)', 'from_4(3) 10(7)', 'from_6(4)', 'from_7(4a)', 'from_8(5)', 'from_9(6)', 'in_1(1)', 'in_1(1)-1', 'in_11(8)', 'in_2(1a)', 'in_3(2)', 'in_4(3)', 'in_5(4)', 'in_6(4a)', 'in_6(4a)-1', 'in_7(5)', 'in_8(6)', 'in_9(7)', 'in_9(7)-1', 'including_1(1)', 'inside_1(1)', 'inside_2(1a)', 'inside_3(1b)', 'inside_5(2)', 'inside_6(n)', 'into_1(1)', 'into_1(1) 3(3)', 'into_1(1)-1', 'into_2(2)', 'into_3(3)', 'into_4(4)', 'into_5(5)', 'into_6(6)', 'into_7(7)', 'into_8(8)', 'near_1(1)', 'near_2(2)', 'near_3(3)', 'near_4(3a)', 'of_10(5a)', 'of_11(6)', 'of_12(6a)', 'of_13(6b)', 'of_14(7)', 'of_15(7a)', 'of_16(7b)', 'of_17(8)', 'of_2(1a)', 'of_3(1b)', 'of_3(1b)-1', 'of_4(2)', 'of_5(2a)', 'of_6(3)', 'of_6(3)-1', 'of_7(3a)', 'of_9(5)', 'off_1(1)', 'off_2(2)', 'off_3(2a)', 'off_4(3)', 'off_5(3a)', 'off_6(3b)', 'on_1(1)', 'on_10(4)', 'on_11(5)', 'on_11(5)-1', 'on_12(6)', 'on_13(6a)', 'on_14(7)', 'on_15(7a)', 'on_16(7b)', 'on_17(8)', 'on_18(8a)', 'on_19(9)', 'on_2(1a)', 'on_20(10)', 'on_22(12)', 'on_3(1b)', 'on_4(1c)', 'on_5(1d)', 'on_7(2)', 'on_8(3)', 'on_8(3) 11(5)', 'on_9(3a)', 'on_9(3a)-1', 'onto_1(1)', 'onto_2(2)', 'onto_3(3)', 'onto_3(n)', 'onto_5(n)', 'onto_6(n)', 'onto_7(n)', 'onto_8(n)', 'over_1(1)', 'over_10(3)', 'over_11(4)', 'over_12(4a)', 'over_13(4b)', 'over_14(5)', 'over_15(6)', 'over_16(7)', 'over_2(1a)', 'over_3(1b)', 'over_4(2)', 'over_6(2b)', 'over_7(2c)', 'over_9(2e)', 'per_1(1)', 'since_1(1)', 'than_1(1)', 'than_2(2)', 'through_1(1)', 'through_1(1) 3(1b)', 'through_10(3)', 'through_10(3)-1', 'through_11(4)', 'through_12(5)', 'through_12(5)-1', 'through_13(5a)', 'through_2(1a)', 'through_3(1b)', 'through_3(1b) 10(3)', 'through_3(1b) 5(1d)', 'through_4(1c)', 'through_5(1d)', 'through_5(1d)-1', 'through_7(2)', 'through_8(2a)', 'through_9(2b)', 'to_1(1)', 'to_10(4a)', 'to_13(5)', 'to_14(6)', 'to_15(7)', 'to_2(1a)', 'to_3(1b)', 'to_5(2)', 'to_6(2a)', 'to_8(3)', 'to_8(3)-1', 'to_9(4)', 'toward_1(1)', 'toward_2(1a)', 'toward_3(1b)', 'toward_4(2)', 'toward_4(2)-1', 'towards_1(1)', 'towards_2(1a)', 'towards_3(1b)', 'towards_4(2)', 'towards_4(2)-1', 'towards_5(3)', 'under_1(1)', 'under_10(4b)', 'under_11(4c)', 'under_12(4d)', 'under_14(5)', 'under_15(5a)', 'under_2(1a)', 'under_3(2)', 'under_4(2a)', 'under_5(2b)', 'under_7(3)', 'under_8(4)', 'under_9(4a)', 'until_1(1)', 'unto_1(1)', 'unto_1(1)-1', 'unto_3(n)', 'unto_4(n)', 'up_1(1)', 'up_2(1a)', 'up_3(2)', 'upon_1(1)', 'upon_11(5)', 'upon_11(5)-1', 'upon_18(8a)', 'upon_2(1a)', 'upon_8(3)', 'upon_9(3a)', 'via_1(1)', 'via_2(1a)', 'via_3(1b)', 'with_1(1)', 'with_1(1)-1', 'with_10(7a)', 'with_11(7b)', 'with_11(7b) 7(5)', 'with_12(7c)', 'with_13(8)', 'with_15(9)-1', 'with_2(2)', 'with_2(2) 3(2a)', 'with_3(2a)', 'with_4(3)', 'with_4(3)-1', 'with_5(3a)', 'with_6(4)', 'with_7(5)', 'with_8(6)', 'with_9(7)', 'without_1(1)', 'without_2(1a)', 'without_3(2)'}
