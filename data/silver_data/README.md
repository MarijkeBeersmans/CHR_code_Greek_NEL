# DOCS for this dataset
## TRISMEGISTOS
The map "Trismegistos expansion" contains:
 - the input: exported from the RE, two files with
  - sources extracted from those entries WITH a TM author id
  - sources extracted those entries WITHOUT a TM author id (normally only persons, but some noise remains)
  - example entry with TM author id: `"338","","Aemilius Paulus","Accius 1","4844","2393","2409","",""`
  - column overview (because the columns are unnamed):
   - 1: RE_id
   - 2: TM authorwork id (almost always empty)
   - 3: the source string
   - 4: the RE article title
   - 5: TM author id
   - 6: start index of the source in the RE entry
   - 7: end index of the source in the RE entry
   - final two columns are irrelevant.
  - example entry without TM author id (extracted on nam id instead): `"12502","","Diod. XVIII 44f. XIX 16.","Attalos 5","","0","1472","1497","unique: 75862"`
   - 1: RE_id
   - 2: TM authorwork id (almost always empty)
   - 3: the source string
   - 4: the RE article title
   - 5: TM author id
   - 6: certainty, this column was for the export and couldn't be -1
   - 7: start index of the source in the RE entry
   - 8: end index of the source in the RE entry
   - 9: TM nam id

 - the script "get_tm_authorwork_ids.py" which queries the TM expansion input and outputs
  - running this takes very long (around 20 hours) and is pretty taxing for the trismegistos thing.

 - the output: Files (both authors and non authors) with the expansion from the TM website.
  - The relevant information that is added can be found in the original_response column:
   -`b'\r\n\r\n\r\n\r\nAUTHORMatch_CONDITIONALAuthorwork@https://www.trismegistos.org/authorwork/130@Diodorus Siculus Bibliotheca@III.11.1 - III.11.1$3$11$1$3$11$1$0`
    -AUTHORMatch_CONDITIONALAuthorwork = type of match/expansion.
    -@https://www.trismegistos.org/authorwork/130 = TM authorwork id
    -@Diodorus Siculus Bibliotheca = human readable title
    -@III.11.1 - III.11.1 = human readable passage
    -$3$11$1$3$11$1$ = machine readable passage, split at the middle dollar sign to find start and end passage. Start and end passage are often the same but not always.
  - 68 241 have an expansion like AUTHORMatch_CONDITIONALAuthorwork.
   - Other results are currently dropped and ignored, including:
    - AuthorMatchMultiple… multiple text options for this source
    - NOT edition not recognized 
    - String(28) and…
    



## Finding GLAUx info
In the folder `GLAUx_search_and_retrieve/scripts` you will find: 
 - The `link_tm_glaux_text.py` script, which links the texts' TM id to the GLAUx text id if the text is present in the GLAUx corpus. 
 - The `add_nam_id_and_lemma_id.py` script, which uses the mappings in the `mappings` folder to add all the Nam Ids associated with the TM_Real id to the source. For every Nam Id, (a) corresponding GLAUx lemma id(s) is/are added
 - The `find_paragraphs_simplified.py` script which:
    - checks whether we can find the paragraph in the GLAUx corpus using the machine readable source from the output of the trismegistos_expansion step
    - if paragraphs is found: checks whether one of the lemma_id's appear in the paragraph
     - note: all paragraphs containing '..' are ignored.
    - if yes, add all the glaux_ids of tokens that have said lemma id in said passage UNLESS it's a "book" type passage.
 -result of this pipeline can be found in: `GLAUx_search_and_retrieve/outputs`, check the latest version, file `GLAUx_search_and_retrieve/outputs/final_mentions_without_books_v4.csv`
 -column overview:
  -RE_id : REAL id
  -tm_authorwork_id : TM authorwork id of the text 
  -source : string of the extracted source
  -RE_artikel : RE article title
  -tm_author_id : tm author id of the individual if present
  -original_response : results of the trismegistos expansion
  -work_name : human-readable title of the text
  -subset : author or non-author 
  -start_passage: start passages from machine readable, if multiple, it is a list joined by commas: eg. 5.2,38.9
  -end_passage: idem for end passage
  -GLAUX_TEXT_ID: text id in GLAUx
  -STRUCT_NC : structure noncontinous (bv. page 234B)
  -STRUCT_C: structure continuous (bv. section 1.2.3)
  -TOKENS : number of tokens in the text according to GLAUx
  -nam_id : list of nam ids associated with the RE_id
  -lemma_id : list of GLAUx lemma ids associated with all the nam ids
  -start_end_passage :  transformation of start_passage and end_passage
  -start_passage_good : start passage actually used to search with in GLAUx
  -end_passage_good : idem for end passage
  -glaux_id : glaux_id of the mention
  -mention_lemma_id : lemma of the mention
  -sentence_id : sentence_id of the mention
  -unit_id : same as GLAUx test id
  -place_in_unit: place of word in the text in GLAUx (e.g. this is the 300th word of the text)
  -word_string_NFC: mention surface form
  -Index: irrelevant
  -startpos: GLaux_id of the first word in the passage 
  -endpos: GLAUx_id of the last word in the passage
  -component_type: type of passage found (e.g. chapter, section, poem...)
  -name_component_level: name/number of the component (e.g. 1.2.3)
  -startpos_place_in_unit: place of first word in passage in the text
  -endpos_place_in_unit: place of last word in passage in the text


## formattting for BLINK 
In the folder `full_final_and_EDA` you will find: 
 - The `adjust_context_window_glaux.py` script with adds the context surrounding the mention and the Kurztext and the Volltext. The output is saved in the `blink_data\version\sythetic` folder.
 - The `train_test_split.py` script which performs the train test split. Silver data is partitioned into train and test sets following the gold data split, ensuring alignment and preventing any overlap of entities between the two datasets. Creates and saves both Voll and Kurz versions.
  - The `batches.py` rearrages the data so there are no duplicates in batches of a certain size. This is for training the bi-encoder.


example output (Kurz) {"context_left": "δὲ τροφῆς ἀποσχόμενος . Κτησίβιος ὁ ἱστοριογράφος ἔτη ἑκατὸν τέσσαρα · ἐν περιπάτῳ δὲ ἐτελεύτα , ὡς Ἀπολλόδωρος ἐν τοῖς Χρονικοῖς δεδήλωκεν . Ἱερώνυμος ὁ συγγραφεὺς πολλοὺς μὲν χρόνους ἐν ταῖς στρατείαις ἀναπλήσας , πλείστοις δὲ τραύμασιν ἐν πολέμοις περιπεσών , ἐτελεύτησεν βιώσας ἔτη ἑκατὸν τέσσαρα , ὥς φησιν", "context_right": "ἐν τῇ ἐννάτῃ τῶν Περὶ τῆς Ἀσίας ἱστοριῶν Γάϊος Λαλλίας Τιωναῖος , Λουκίου υἱός , πόλεως Βονωνίας , ἔτη ἑκατὸν πέντε . Πόπλιος † χουϊσέντιος † Ἐφυρίων , Ποπλίου ἀπελεύθερος , πόλεως Βονωνίας , ἔτη ἑκατὸν πέντε . Τίτος Κοττίνας Χρύσανθος , Τίτου ἀπελεύθερος , πόλεως Φαουεντίας ,", "mention": "Ἀγαθαρχίδης", "data_status": "silver", "text": "A. von Knidos, Historiker und Geograph", "label_id": "904", "RE_id": "1742", "label_title": "Agatharchides 3", "glaux_id": "118181899"}


