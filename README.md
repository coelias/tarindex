# tarindex
tarindex, UNIX tar file indexer for python

This tool is aimed to operate with high density Tar files (millions of files within the tar file).

tarindex parses the whole tar file indexing (only files with size > 0) and extends the Tar file itself
with an extra file containing the index gzipped, so it can be loaded later on much faster.

The index file added into tar file has the following format: .tarindex-NNNN, as it is possible
to have several indexes if the tar file is extended in the future.

Example of usage:


```python
>>> from tarindex import TarFileIdx

>>> tf=TarFileIdx('file.tar')
Indexing tar file... DONE!
291001 files indexed

# Printing all filenames
>>> for i in tf.getNames():
>>>    print i

# Extract individual file
>>> print tf.getFile('path/to/file').read()

# Extract individual to disc
>>> print tf.getLocalFile('path/to/file',directory='/tmp')

#Extracting files into strings
>>> for i in tf.iterFiles():
>>>    file_contents=i.read()

#Extracting files into strings (only python files)
>>> for i in tf.iterFiles(regex='\.py$'):
>>>    file_contents=i.read()

#Extracting the files into filesystem and deleting them afterwards
>>> for i in tf.iterLocalFiles():
>>>    file_contents=open(i).read()
# Files get deleted automatically (tempfile)

#Extracting the files into filesystem and deleting them afterwards
#Fast access on ram disk
>>> for i in tf.iterLocalFiles(directory='/dev/shm'):
>>>    file_contents=open(i).read()
# Files get deleted automatically (tempfile)
```
