# tarindex
tarindex, UNIX tar file indexer for python

This tool is aimed to operate with high density Tar files (millions of files within the tar file).

NOTE: tarindex does not work with gzipped/bzipped tar files.

tarindex parses the whole tar file indexing (only files with size > 0) and extends the Tar file itself
with an extra file containing the gzipped index, so it can be loaded later on much faster.

The index file added into tar file has the following format: .tarindex-NNNN, as it is possible
to have several indexes if the tar file is extended in the future.

Installation:

```bash
$ pip install tarindex
```

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

# If you want to remove the index from the tar file...
>>> tf.deleteIndex()

```

```bash

# Performance example

$ cat /tmp/a.py
from tarindex import TarFileIdx
import sys
TarFileIdx(sys.argv[1])

$ ls -lh reads.tar
-rw-rw-r-- 1 compass 2041577985 **230G** Oct 11 22:19 reads.tar

$ time python /tmp/a.py reads.tar
Indexing tar file... 100.0% done
DONE!

real	6m23.469s
user	0m19.451s
sys	1m20.264s


$ time python /tmp/a.py reads.tar
Loading index 4.9Mb gziped... Done!

real	0m0.552s
user	0m0.311s
sys	0m0.172s
```
