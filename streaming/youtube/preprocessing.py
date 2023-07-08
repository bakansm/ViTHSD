from underthesea import word_tokenize
import pandas as pd
TEENCODE = '/home/ubuntu/ViTHSD-Vietnamese-Targeted-Hate-Speech-Detection/preprocess_data/teencode.txt'
teencode_df = pd.read_csv(TEENCODE,names=['teencode','map'],sep='\t',)
teencode_list = teencode_df['teencode'].to_list()
map_list = teencode_df['map'].to_list()
def searchTeencode(word):
  try:
    global teencode_count
    index = teencode_list.index(word)
    map_word = map_list[index]
    teencode_count += 1
    return map_word
  except:
    pass
    
STOPWORDS = '/home/ubuntu/ViTHSD-Vietnamese-Targeted-Hate-Speech-Detection/preprocess_data/vietnamese-stopwords-dash.txt'

with open(STOPWORDS, "r") as ins:
    stopword = []
    for line in ins:
        stopword.append(line.strip('\n'))

def remove_stopwords(line):
    global stopword_count
    words = []
    for word in line.strip().split():
        if word not in stopword:
            words.append(word)
        if word in stopword:
            stopword_count += 1
    return ' '.join(words)
    
stopword_count = 0
teencode_count =0
def preprocess(sentence):
  lenn = 0
  sentence = str(sentence)
  #Tokenize
  List_tokens = word_tokenize(sentence,format='text')
  List_tokens = word_tokenize(List_tokens)

  #Teencode
  for tokens_idx, text_tokens in enumerate(List_tokens):
    deteencoded = searchTeencode(text_tokens)
    if (deteencoded != None):
        List_tokens[tokens_idx] = deteencoded

  deteencode_sentence = (" ").join(List_tokens)
  
  #Stopwords
  tokens_without_sw = remove_stopwords(deteencode_sentence)

  return tokens_without_sw