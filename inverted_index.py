#Jonathan S. Chandranathan
#Takes in as an input a list of books (in the format [book_id, text]) and outputs an inverted index dictionary.  In the index, each word is associated with a list of the books in which that word appears.

import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    book_id = record[0]
    text = record[1]
    words = text.split()
    for word in words:
      mr.emit_intermediate(word, book_id)

def reducer(word, book_id):
    inverted_index = []
    for ids in book_id:
        if ids not in inverted_index:
            inverted_index.append(ids)
    mr.emit((word, inverted_index))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
