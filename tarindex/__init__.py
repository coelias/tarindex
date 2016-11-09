##    This program is free software: you can redistribute it and/or modify
##    it under the terms of the GNU General Public License as published by
##    the Free Software Foundation, either version 3 of the License, or
##    (at your option) any later version.
##
##    This program is distributed in the hope that it will be useful,
##    but WITHOUT ANY WARRANTY; without even the implied warranty of
##    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##    GNU General Public License for more details.
##
##    You should have received a copy of the GNU General Public License
##    along with this program.  If not, see <http://www.gnu.org/licenses/>.

##    Author: Carlos del Ojo Elias (deepbit@gmail.com)

REGTYPE= '0'         #   /* regular file */
AREGTYPE='\0'       #   /* regular file */
LNKTYPE= '1'         #   /* link */
SYMTYPE= '2'         #   /* reserved */
CHRTYPE= '3'         #   /* character special */
BLKTYPE= '4'         #   /* block special */
DIRTYPE= '5'         #   /* directory */
FIFOTYPE='6'        #   /* FIFO special */
CONTTYPE='7'        #   /* reserved */


from StringIO import StringIO
import gzip
import os
import pickle
import random
import re
import struct
import sys
import array
import time

import threading 

TARACCESS=threading.RLock()

def allDirs(p):
	oldp=None
	while p!=oldp:
		oldp=p
		p=os.path.split(p)[0]
		yield p

class SortedList:
	def __init__(self,data=[]):
		self.lst=data

	def sort(self):
		self.lst.sort()

	def __len__(self):
		return len(self.lst)

	def __getitem__(self,k):
		return self.lst[k]

	def append(self,el):
		ini=0
		end=len(self.lst)

		if not end:
			self.lst.append(el)
			return 0

		while ini+1<end:
			mid=(ini+end)/2
			midel=self.lst[mid]
			if el<=midel: end=mid
			else: ini=mid

		if el<self.lst[ini]:
			self.lst.insert(ini,el)
			return ini
		else:
			self.lst.insert(ini+1,el)
			return ini+1

	def contains_substring(self,el):
		ini=0
		end=len(self.lst)

		while ini+1<end:
			mid=(ini+end)/2
			midel=self.lst[mid]
			if el<midel: end=mid
			elif el>midel: ini=mid
			else: return True

		if el in self.lst[ini]: return True
		if el in self.lst[end]: return True

		return False

	def __contains__(self,el):
		ini=0
		end=len(self.lst)

		while ini+1<end:
			mid=(ini+end)/2
			midel=self.lst[mid]
			if el<midel: end=mid
			elif el>midel: ini=mid
			else: return True

		if el==self.lst[ini]: return True
		if el==self.lst[end]: return True

		return False
	
	def __iter__(self):
		return self.lst.__iter__()

class TarFile:
	def __init__(self,fd,pos,size,name=None):
		self.fd=fd
		self.pos=pos
		self.size=size
		self.curpos=0
		self.name=name

	def read(self,sz=0):
		if sz==0:
			TARACCESS.acquire()
			self.fd.seek(self.pos+self.curpos)
			res=self.fd.read(self.size-self.curpos)
			self.curpos+=len(res)
			TARACCESS.release()
		else:
			TARACCESS.acquire()
			self.fd.seek(self.pos+self.curpos)
			res=self.fd.read(min(self.size-self.curpos,sz))
			self.curpos+=len(res)
			TARACCESS.release()
		return res
	
	def __iter__(self):
		data=""
		pos=0
		while True:
			npos=data.find('\n',pos)
			if npos==-1:
				if self.curpos>=self.size: break
				data=data[pos:]
				pos=0
				data+=self.read(131072)
			else:
				yield data[pos:npos+1]
				pos=npos+1

		data=data[pos:]
		if data:
			yield data

	def seek(self,pos,whence):
		if not whence:
			self.curpos=min(pos,self.size)
		if whence==1:
			self.curpos=max(0,min(self.curpos+pos,self.size))
		elif whence==2:
			self.curpos=min(self.size+pos,self.size)

	def tell(self):
		return self.curpos


def indexFromTar(f):
	sys.stderr.write('Indexing tar file... ')
	filesdone=0
	flen=os.stat(f).st_size
	assert not flen%512
	nblocks=flen/512
	a=open(f)
	
	block=0
	xfname=None
	
	index=[]
	dirs=set()
	
	while block<nblocks:
		xfound=False
		a.seek(block*512,0)
		data=a.read(512)
	
		name=data[:100].strip('\x00')
		lname=data[157:257].strip('\x00')
		prefix=data[345:500].strip('\x00')
		tf=data[156]

		if prefix:
			name=os.path.join(prefix,name)

	
		try:
			sz=int(data[124:136].strip('\0'),8)
		except:
#			print 'error',hex(block*512),repr(data[124:136]),name
			block+=1
			if not xfound: xfname=None
			continue
	
		block+=1
	
		if sz:
			if sz%512: bltoread=(512-(sz%512)+sz)/512
			else: bltoread=sz/512
			if tf=='x' or name.startswith('./PaxHeader'):
				xfound=True
				xfname='='.join(a.read(sz).split('\n')[0].split('=')[1:])
				xfname=xfname.strip('\x00')
			elif tf=='L':
				xfound=True
				xfname=a.read(sz).strip('\x00')
			elif tf=='0':
				if xfname: index.append([xfname,a.tell(),sz])
				else: index.append([name,a.tell(),sz])
				filesdone+=1
				if not filesdone%1000:
					sys.stderr.write('\rIndexing tar file... {0:.1f}% done'.format(float(a.tell())/flen*100))

			block+=bltoread

		elif tf=='5':
			if xfname: dirs.add(xfname.rstrip('/').rstrip('\\'))
			else: dirs.add(name.rstrip('/').rstrip('\\'))
				
		
		if not xfound: xfname=None

	a.close()

	for i,_,_ in index:
		for i in allDirs(i):
			dirs.discard(i)

	dirs=sorted(dirs)
	index.sort()
	return index,dirs

def safe_dump(n):
	data=""
	for i in range(8):
		data+=struct.pack('B',n&255)
		n>>=8
	
	return data

def safe_load(s):
	n=0
	p=0
	for i in s:
		n|=struct.unpack('B',i)[0]<<(p*8)
		p+=1
	return n

class Index:
		def __init__(self,data=None,positions=None,sizes=None,names=None,dirs=None):
			if data:
					self.totitems=safe_load(data[:8])
		
					self.positions=struct.unpack('Q'*self.totitems,data[8:8+self.totitems*8])
					self.sizes=struct.unpack('Q'*self.totitems,data[8+self.totitems*8:8+self.totitems*16])
		
					self.names=data[8+self.totitems*16:].split("\n")
					self.names,self.dirs=SortedList(self.names[:len(self.sizes)]),SortedList(self.names[len(self.sizes):])
			else:
					self.totitems=len(names)
					self.positions=positions
					self.sizes=sizes
					self.names=SortedList(names)
					if not dirs: self.dirs=SortedList()
					else: self.dirs=SortedList(dirs)

		def __len__(self):
			return self.totitems

		def splitPath(self,path):
			objs=[]
			prev=None
			while path!=prev:
				prev=path
				path,obj=os.path.split(path)
				if obj: objs.insert(0,obj)
			objs.insert(0,'/')
			return objs

		def getName(self,p):
			return self.names[p]

		def addFile(self,name,pos,size):
			assert name not in self.names
			p=self.names.append(name)
			self.positions.insert(p,pos)
			self.sizes.insert(p,size)
			self.totitems+=1

		def addDir(self,name):
			if name not in self.dirs: self.dirs.append(name)

		def __getitem__(self,name):
			ini=0
			end=self.totitems
			while ini<end:
				mid=(end+ini)/2
				midname=self.getName(mid)
				if name<midname:
					end=mid
				elif name>midname:
					ini=mid
				else:
					return [name,self.sizes[mid],self.positions[mid]]

			raise Exception('File not found: {}'.format(name))

		def __iter__(self):
			for i in xrange(self.totitems):
				yield [self.getName(i),self.sizes[i],self.positions[i]]


class TarFileIdx:
	MAGICNUMBER1='\x47\x13\x38\x03\x11\x72\x72'[::-1]
	MAGICNUMBER2='\x47\x13\x38\x03\x11\x72\x72'*3

	def __init__(self,mode='wo',tarfilepath=None):
		'''Opens a tar file
		:param tarfilepath: path to the tar file to be opened
		:param mode: opening mode [(r)ead,(w)rite,std(i)n,std(o)ut]
		'''
		new=False
		if not os.path.isfile(tarfilepath):
			new=True

		MODES={'r':'Reads from a file',
		       'w':'Writes to a file, allows reading',
			   'wo':'Writes into stdout [and a file, if provided], doesn\'t allow reading if there is not file'}

		if mode not in MODES:
			raise Exception('Mode not allowed {}'.format(MODES))

		self.mode=mode
		self.index=None
		self.tarfilePath=tarfilepath

		if self.mode=='r':
			self.tarfile=open(tarfilepath)
		elif self.mode=='w':
			self.tarfile=open(tarfilepath,'a+')
		elif self.mode=='wo':
			if tarfilepath: 
				if os.path.exists(self.tarfilePath):
					raise Exception('wo mode requires a new file, and {} already exists'.format(self.tarfilePath))
				self.tarfile=open(tarfilepath,'a+')
			else: self.tarfile=None

		if self.mode=='r':
			if self.__findIndexPos()==-1:
					self.__createIndex()
			else:
					self.__loadIndex()
		if self.mode=='w' and not new:
			self.__removeCap()

			if self.__findIndexPos()==-1:
					self.__createIndex()
			else:
					self.__loadIndex()
					self.deleteIndex()

		if self.mode in ['w','wo'] and new:
			self.index=Index(names=[],positions=[],dirs=[],sizes=[])
			self.updateTar("./",data='',entryType=DIRTYPE)

	def __findIndexPos(self):
		assert self.tarfile

		self.tarfile.seek(-512,2)
		info=self.tarfile.read()

		if not TarFileIdx.MAGICNUMBER2 in info: 
			return -1

		lastmn=info.find(TarFileIdx.MAGICNUMBER2)
		

		return safe_load(info[lastmn-8:lastmn])+len(TarFileIdx.MAGICNUMBER1)+(len(info)-lastmn)+8

	def iterLocalFiles(self,directory="./",delete=True,regex=None):
		assert self.tarfile
		if regex: regex=re.compile(regex)
		for ti in self.index:
			if regex and not regex.findall(ti.name): continue
			dest=self.getLocalFile(ti,directory=directory,createparents=False)
			yield dest
			if delete: os.unlink(dest)

	def __len__(self):
		return len(self.index)

	def iterFiles(self,regex=None):
		assert self.tarfile
		if regex: regex=re.compile(regex)
		for ti in self.index:
			if regex and not regex.findall(ti.name): continue
			yield self.getFile(ti)

	def getFile(self,ti):
		assert self.tarfile
		if type(ti)==str:
			ti=self.index[ti]

		name,size,pos=ti

		return TarFile(self.tarfile,pos,size,name) 

	def getLocalFile(self,ti,directory='./',createparents=True):
		assert self.tarfile
		if type(ti)==str:
			ti=self.index[ti]

		parents,fil=os.path.split(ti[0])

		if createparents:
			try:os.makedirs(os.path.join(directory,parents))
			except OSError as e:
				if e.errno!=17: raise e
			dest=os.path.join(directory,parents,fil)
		else:
			dest=os.path.join(directory,fil)

		if os.path.exists(dest):
			raise Exception('File already exists, we do not overwrite!')
		fdest=open(dest,'w')
		fdest.write(self.getFile(ti).read())
		fdest.close()

		return dest

	def getMembers(self):
		for i in self.index:
			yield i

	def getNames(self):
		return (i.name for i in self.index)

        def __iter__(self):
            return self.index.__iter__()

	def getMember(self,name):
			return self.index[name]
	__getitem__=getMember

	def __loadIndex(self):
		assert self.tarfile
		idxpos=self.__findIndexPos()
		self.tarfile.seek(-idxpos,2)
		data=self.tarfile.read()

		sys.stderr.write('Loading index {0:.1f}Mb gziped... '.format(float(len(data))/(1024*1024)))

		assert data[:len(TarFileIdx.MAGICNUMBER1)]==TarFileIdx.MAGICNUMBER1

		data=data[len(TarFileIdx.MAGICNUMBER1):data.find(TarFileIdx.MAGICNUMBER2)-8]
		gzbuff=StringIO(data)
		gzf=gzip.GzipFile(fileobj=gzbuff)
		data=gzf.read()
		self.index=Index(data)

		sys.stderr.write('Done!\n')

	def __createIndex(self):
		fils,dirs=indexFromTar(self.tarfilePath)

		self.index=Index(positions=[i[1] for i in fils],sizes=[i[2] for i in fils],names=[i[0] for i in fils],dirs=dirs)

		sys.stderr.write('DONE!\n')

	def __storeIndex(self):
		totitems=len(self.index.names)

		d2=safe_dump(totitems)+struct.pack('Q'*totitems,*self.index.positions)+struct.pack('Q'*totitems,*self.index.sizes)+'\n'.join(self.index.names)+'\n'+'\n'.join(self.index.dirs)
		
		z=StringIO(d2)

		gzbuff=StringIO()
		gzinfo=gzip.GzipFile(fileobj=gzbuff,mode='w')
		gzinfo.write(z.read())
		gzinfo.close()
		gzbuff.seek(0)
		gzinfo=gzbuff.read()

		idx=TarFileIdx.MAGICNUMBER1+gzinfo+safe_dump(len(gzinfo))+TarFileIdx.MAGICNUMBER2
		fname="./tarFilIdx-{0}".format(random.randint(1000,9999))

		self.updateTar(fname,idx)

	def deleteIndex(self):
		assert self.mode=='w'
		bl=1
		
		while True:
			self.tarfile.seek(-bl*512,2)
			d=self.tarfile.read(512)
			if d[257:262]=='ustar':
				break
			bl+=1
		
		if d.startswith('./tarFilIdx-'):
			pos=bl*512
			sz=os.fstat(self.tarfile.fileno()).st_size
			self.tarfile.truncate(sz-pos)

	def updateTar(self,path,data,entryType='0',mode=511,uid=1000,gid=1000,mtime=int(time.time())):
		assert self.mode!='r'

		fname=path[:99]
		fname+='\x00'*(100-len(fname))

		datasize="{0:0>11}".format(oct(len(data)))[-11:]
		mode="{0:0>7}".format(oct(mode))[-7:]
		uid="{0:0>7}".format(oct(uid))[-7:]
		gid="{0:0>7}".format(oct(gid))[-7:]
		mtime="{0:0>11}".format(oct(mtime))[-11:]



		part1=("{0}\x00{1}\x00{2}\x00"+datasize+"\x00{3}\x00").format(mode,uid,gid,mtime)

		part2='{}\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00ustar  \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000000000\x000000000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'.format(entryType)

		cksum=oct(sum(struct.unpack('B'*504,fname+part1+part2))+32*8)
		cksum="{0: >7}".format(cksum)[-7:]+'\x00'

		header=fname+part1+cksum+part2

		TARACCESS.acquire()
		self.writeOutput(header)
		if data:
			self.writeOutput(data)
			self.writeOutput('\x00'*(512-(len(data)%512)))
		TARACCESS.release()

	def __removeCap(self):
		endb=1

		while True:
			self.tarfile.seek(-endb*512,2)
			bl=self.tarfile.read(512)
			if bl[156] in '\x0001234567xg' and bl[257:262]=='ustar':
				sz=int(bl[124:135],8)
				if sz%512: sz=sz+512-(sz%512)
				updatepos=(endb-sz/512-1)*512
				break
			endb+=1

		self.tarfile.seek(-updatepos,2)
		if set(self.tarfile.read())!=set('\00'):
			raise Exception

		self.tarfile.truncate(os.fstat(self.tarfile.fileno()).st_size-updatepos)

	
	def __closeTar(self):
		TARACCESS.acquire()
		if self.mode in ['w','wo']:
			self.__storeIndex()
			print 'aaa'
			self.writeOutput('\x00'*1024)
		TARACCESS.release()

	def close(self):
		if self.mode in ['w','wo']:
			self.__closeTar()
		self.tarfile.close()

	def writeOutput(self,data):
		assert self.mode in ['w','wo']
		if self.tarfile:
			self.tarfile.write(data)
			self.tarfile.flush()
		if self.mode=='wo':
			sys.stdout.write(data)
			sys.stdout.flush()

if  __name__=='__main__':
	a=TarFileIdx('w',sys.argv[1])
	sys.stderr.write('{} {}\n'.format(len(a),'files indexed'))
	a.close()


