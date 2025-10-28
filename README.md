----
CODE FOR THE PAPER "Automatic Named Entity Linking for Ancient Greek with a Domain-Specific Knowledge Base" to be presented at CHR 2025 (Luxembourg)
----
This repo accompanies the paper, in which we attempt to use the BLINK architecture to link Ancient Greek texts to a domain specific knowledge base. It contains a minimal codebase to reproduce the results obtained in the paper, in addition to other scripts that we used for transparancy.   
Note: Re-training is not possible, since only the test set of the manually annotated (gold) data is included as per the annotators' request. The full annotations will be published in a more complete version.


## Directory Overview
- **adjusted_blink_files**: Contains code based on the BLINK repository with adjustments.
- **blink_training**: Contains the training scripts.
- **data**:
  - **silver_data**: Contains the scripts to create the silver data. These cannot be reproduced since the TM algorithm and the GLAUx-backend are not available to the public, but the scripts and intermediate steps (with this information removed) are included for transparency.
  - **blink_data**: Contains the test sets the models were tested on.
- **nel-baseline-lemma**: Contains the script to run the baseline.
- **Paulys_kb**: Contains the version of the RE used for these experiments.
- **results**: Contains CSV files of the results and TXT files of the scores.


## How to use:
### Create the environment
```bash
conda env create -f environment.yml
```

### Download the models
```bash
bash dowload_models.py
```

### Testing
```bash
bash blink_training/CHR_testing.sh
```

## Credits
We thank the following projects and people for making this research possible:

### Gold data annotators:
- Evelien de Graaf: [ORCID](https://orcid.org/0009-0006-8650-1595)
- Herbert Verreth: [ORCID](https://orcid.org/0000-0002-3538-8290)

### Silver data preliminaries:
**GLAUx corpus**: [link](https://glaux.be/)

```bibtex
 @inproceedings{Keersmaekers_2021, 
address={Online}, 
title={The GLAUx corpus: methodological issues in designing a long-term, diverse, multi-layered corpus of Ancient Greek}, 
booktitle={Proceedings of the 2nd International Workshop on Computational Approaches to Historical Language Change 2021},
doi={10.18653/v1/2021.lchange-1.6},
publisher={Association for Computational Linguistics}, 
author={Keersmaekers, Alek}, 
year={2021}, 
month=aug, 
pages={39–50} }
```

**TM database** [link](https://www.trismegistos.org/)

```bibtex
@inproceedings{depauw_trismegistos_2014,
	address = {Cham},
	series = {Communications in {Computer} and {Information} {Science}},
	title = {Trismegistos: {An} {Interdisciplinary} {Platform} for {Ancient} {World} {Texts} and {Related} {Information}},
	isbn = {978-3-319-08425-1},
	shorttitle = {Trismegistos},
	doi = {10.1007/978-3-319-08425-1_5},
	language = {en},
	booktitle = {Theory and {Practice} of {Digital} {Libraries} -- {TPDL} 2013 {Selected} {Workshops}},
	publisher = {Springer International Publishing},
	author = {Depauw, Mark and Gheldof, Tom},
	editor = {Bolikowski, Łukasz and Casarosa, Vittore and Goodale, Paula and Houssos, Nikos and Manghi, Paolo and Schirrwagen, Jochen},
	year = {2014},
	pages = {40--52},
}
```

### BLINK
```bibtex
@inproceedings{wu2019zero,
 title={Zero-shot Entity Linking with Dense Entity Retrieval},
 author={Ledell Wu, Fabio Petroni, Martin Josifoski, Sebastian Riedel, Luke Zettlemoyer},
 booktitle={EMNLP},
 year={2020}
}
```
[Repo](https://github.com/facebookresearch/BLINK.git)

### Wikisource RE
[Wikisource](https://de.wikisource.org/wiki/Paulys_Realencyclop%C3%A4die_der_classischen_Altertumswissenschaft)
