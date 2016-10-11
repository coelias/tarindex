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


from StringIO import StringIO
import gzip
import os
import pickle
import random
import re
import struct
import sys
import tarfile

def updateTar(tarpath,fname,data):
	fname+='\x00'*(100-len(fname))
	datasize=oct(len(data))
	datasize='0'*(11-len(datasize))+datasize
	
	a=open(tarpath)
	endb=1
	
	while True:
		a.seek(-endb*512,2)
		bl=a.read(512)
		if bl[156] in '\x0001234567xg' and bl[257:262]=='ustar':
			sz=int(bl[124:135],8)
			if sz%512: sz=sz+512-(sz%512)
			updatepos=(endb-sz/512-1)*512
			break
		endb+=1
	
	a.close()
	
	
	
	part1='0000644\x000000000\x000000000\x00'+datasize+'\x0000000000000\x00'
	
	part2='0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00ustar  \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000000000\x000000000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
	
	cksum=oct(sum(struct.unpack('B'*504,part1+fname+part2))+32*8)+'\x00 '
	
	header=fname+part1+cksum+part2
	
	a=open(tarpath,'a+')
	a.seek(-updatepos,2)
	if set(a.read())!=set('\00'):
		raise Exception
	a.truncate(os.fstat(a.fileno()).st_size-updatepos)
	a.write(header)
	a.write(data)
	a.write('\x00'*(512-(len(data)%512)))
	a.write('\x00'*1024)
	a.close()


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
	
	while block<nblocks:
		xfound=False
		a.seek(block*512,0)
		data=a.read(512)
	
		name=data[:100].strip('\x00')
		lname=data[157:257]
		pf=data[345:354+155]
		tf=data[156]

	
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
			if tf=='x':
				xfound=True
				xfname='='.join(a.read(sz).split('\n')[0].split('=')[1:])
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
		
		if not xfound: xfname=None

	a.close()
	
	return index

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
		def __init__(self,data=None,positions=None,sizes=None,names=None):
			if data:
					self.totitems=safe_load(data[:8])
		
					self.positions=struct.unpack('Q'*self.totitems,data[8:8+self.totitems*8])
					self.sizes=struct.unpack('Q'*self.totitems,data[8+self.totitems*8:8+self.totitems*16])
					self.namepos=struct.unpack('Q'*self.totitems,data[8+self.totitems*16:8+self.totitems*24])
		
					self.names=data[8+self.totitems*24:]+"\n"
			else:
					self.totitems=len(names)
					self.positions=positions
					self.sizes=sizes
					self.namepos=[]
					self.names=names

					np=0
					for i in self.names:
						self.namepos.append(np)
						np+=len(i)+1

					self.names='\n'.join(self.names)+'\n'

		def __len__(self):
			return self.totitems

		def getName(self,p):
			ini=self.namepos[p]
			end=self.names.find('\n',ini)

			return self.names[ini:end]

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
					ti=tarfile.TarInfo()
					ti.name=name
					ti.size=self.sizes[mid]
					ti.offset_data=self.positions[mid]
					return ti

			raise Exception

		def __iter__(self):
			for i in xrange(self.totitems):
				ti=tarfile.TarInfo()
				ti.name=self.getName(i)
				ti.size=self.sizes[i]
				ti.offset_data=self.positions[i]
				yield ti


class TarFileIdx:
	MAGICNUMBER1='\x47\x13\x38\x03\x11\x72\x72'[::-1]
	MAGICNUMBER2='\x47\x13\x38\x03\x11\x72\x72'*3

	def __init__(self,tarfilepath):
		self.index=None
		self.tarfilePath=tarfilepath

		if self.__findIndexPos()==-1:
				self.__createIndex()
		else:
				self.__loadIndex()

		self.tarfile=tarfile.TarFile(tarfilepath,'r')

	def __findIndexPos(self):
		fp=open(self.tarfilePath)
		fp.seek(-4096,2)
		info=fp.read()
		fp.close()

		if not TarFileIdx.MAGICNUMBER2 in info: 
			return -1

		lastmn=info.find(TarFileIdx.MAGICNUMBER2)
		

		return safe_load(info[lastmn-8:lastmn])+len(TarFileIdx.MAGICNUMBER1)+(len(info)-lastmn)+8

	def iterLocalFiles(self,directory="./",delete=True,regex=None):
		if regex: regex=re.compile(regex)
		for ti in self.index:
			if regex and not regex.findall(ti.name): continue
			dest=self.getLocalFile(ti,directory=directory,createparents=False)
			yield dest
			if delete: os.unlink(dest)

	def __len__(self):
		return len(self.index)

	def iterFiles(self,regex=None):
		if regex: regex=re.compile(regex)
		for ti in self.index:
			if regex and not regex.findall(ti.name): continue
			yield self.getFile(ti)

	def getFile(self,ti):
		if type(ti)==str:
			ti=self.index[ti]

		return self.tarfile.extractfile(ti)

	def getLocalFile(self,ti,directory='./',createparents=True):
		if type(ti)==str:
			ti=self.index[ti]

		parents,fil=os.path.split(ti.name)

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
		idxpos=self.__findIndexPos()
		fp=open(self.tarfilePath)
		fp.seek(-idxpos,2)
		data=fp.read()
		fp.close()

		sys.stdout.write('Loading index {0:.1f}Mb gziped... '.format(float(len(data))/(1024*1024)))

		assert data[:len(TarFileIdx.MAGICNUMBER1)]==TarFileIdx.MAGICNUMBER1

		data=data[len(TarFileIdx.MAGICNUMBER1):data.find(TarFileIdx.MAGICNUMBER2)-8]
		gzbuff=StringIO(data)
		gzf=gzip.GzipFile(fileobj=gzbuff)
		data=gzf.read()
		self.index=Index(data)

		sys.stdout.write('Done!\n')

	def __createIndex(self):

		d=sorted(indexFromTar(self.tarfilePath))


		totitems=len(d)
		np=0
		namepositions=[]
		for i,_,_ in d:
			namepositions.append(np)
			np+=len(i)+1

		d2=safe_dump(len(d))+struct.pack('Q'*totitems,*[i[1] for i in d])+struct.pack('Q'*totitems,*[i[2] for i in d])+struct.pack('Q'*totitems,*namepositions)+'\n'.join([i[0] for i in d])
		
		z=StringIO(d2)

		gzbuff=StringIO()
		gzinfo=gzip.GzipFile(fileobj=gzbuff,mode='w')
		gzinfo.write(z.read())
		gzinfo.close()
		gzbuff.seek(0)
		gzinfo=gzbuff.read()

		idx=TarFileIdx.MAGICNUMBER1+gzinfo+safe_dump(len(gzinfo))+TarFileIdx.MAGICNUMBER2
		fname=".tarFilIdx-{0}".format(random.randint(1000,9999))

		try:
			updateTar(self.tarfilePath,fname,idx)
		except:
			sys.stderr.write('WARNING: Could not write the index into the tar file!\n')

		self.index=Index(positions=[i[1] for i in d],sizes=[i[2] for i in d],names=[i[0] for i in d])


		sys.stderr.write('DONE!\n')

	def deleteIndex(self):
		a=open(self.tarfilePath,'a+')
		bl=1
		
		while True:
			a.seek(-bl*512,2)
			d=a.read(512)
			if d[257:262]=='ustar':
				break
			bl+=1
		
		if d.startswith('.tarFilIdx-'):
			pos=bl*512
			sz=os.fstat(a.fileno()).st_size
			a.truncate(sz-pos)
			a.write('\x00'*1024)
		a.close()


if  __name__=='__main__':
	a=TarFileIdx(sys.argv[1])
	print len(a),'files indexed'
