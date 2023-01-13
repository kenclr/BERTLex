# Introduction

**BERTological Lexicography for Prepositions** : Contextualized word embedding (CWE) models such as BERT (Devlin et al. (2019)) [[1]](#1) have been used in many NLP tasks. Gessler and
Schneider (2021) (G&S) [[2]](#2) focus on the use of BERT in disambiguating
rare senses of nouns, verbs, and prepositions. In examining preposition disambiguation, their results are comparable to earlier efforts and
achieving better results than problems described in Litkowski (2013) [[3]](#3).
Earlier efforts achieved results using traditional NLP methods, characterizing syntactic and semantic behaviors. BERTology provides a new
resource and additional perspective that might assist in usual lexicographic procedures. We begin exactly at the place where G&S ended,
using its methods, adding the further property of keeping a link to the
instances in the Pattern Dictionary of English Prepositions (PDEP,
Litkowski (2014)) [[4]](#4), enabling further lexicographic analysis.

This is the code accompanying the paper (see [[5]](#5) [Citation](#citation)) [_BERTological Lexicography for Prepositions_][(http://www.clres.com/online-papers/BERTLex.pdf)].

# Usage
## Data
While the code included here can be used to develop the dataframe for the preposition predictions, the code and the full data are also contained a zipped file available at https://www.clres.com/online-papers/BERTLex.zip. This includes the tab-separated value file **pdep-bert-base-cased_7.tsv** which can be loading into a dataframe that contains the predictions. In particular, each of the labels in the file identify the corpus instances, the basic data for the lexicographic analyses in the paper.

## Initial Setup
Our code builds on the code from the G&S paper, which provides a link to its code, at https://github.com/lgessler/bert-has-uncommon-sense. You need to implement the steps through its **Setup**. Since we are not implementing all of its steps, you need to pay attention with the differences. In particularly, you need only its **Step 2** (Create a new Anaconda environment: creating the environment with **bhus**) and **Step 3** (Install dependencies: **requirements.txt**). The setup establishes the folders consistent with those of G&S, which also makes use of various functions.

# Files
- README.md
- ranks.py

# References

- <a id="1">[1]</a> Jacob Devlin, Ming-Wei Chang, Kenton Lee, and Kristina Toutanova.
BERT: Pre-training of deep bidirectional transformers for language understanding. In Proceedings of the 2019 Conference of the North American
Chapter of the Association for Computational Linguistics: Human Language Technologies, Volume 1 (Long and Short Papers), pages 4171–4186,
Minneapolis, Minnesota, June 2019. Association for Computational Linguistics. doi: 10.18653/v1/N19-1423. URL https://aclanthology.org/N19-1423.
- <a id="2">[2]</a> Luke Gessler and Nathan Schneider. BERT has uncommon sense: Similarity ranking for word sense BERTology. In Proceedings of the
Fourth BlackboxNLP Workshop on Analyzing and Interpreting Neural
Networks for NLP, pages 539–547, Punta Cana, Dominican Republic, November 2021. Association for Computational Linguistics. doi:
10.18653/v1/2021.blackboxnlp-1.43. URL https://aclanthology.org/2021.blackboxnlp-1.43.
- <a id="3">[3]</a> Ken Litkowski. Preposition disambiguation: Still a problem. Technical
Report 13-02, CL Research, Damascus, MD 20872 USA, September 2013.
URL http://www.clres.com/online-papers/PrepWSD2013.pdf.
- <a id="4">[4]</a> Ken Litkowski. Pattern Dictionary of English Prepositions. In Proceedings of the 52nd Annual Meeting of the Association for Computational Linguistics (Volume 1: Long Papers), pages 1274–1283, Baltimore,
Maryland, June 2014. Association for Computational Linguistics. URL
http://www.aclweb.org/anthology/P14-1120.
- <a id="5">[5]</a> Ken Litkowski. BERTological Lexicography for Prepositions. Working
Paper 22-02, CL Research, Damascus, MD 20872 USA, December 2022.
URL http://www.clres.com/online-papers/BERTLex.pdf.

# Citation
To cite the paper
```
@techreport{TagPDEP2SST,
    title = {BERTological Lexicography for Prepositions},
    number = {22-02},
    author = {Litkowski, Ken},
    institution = {CL Research},
    address = {Damascus, MD 20872 USA},
    year = {2022},
    url = {http://www.clres.com/online-papers/BERTLex.pdf}
}
```

