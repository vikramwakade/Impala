#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <typeinfo>
#include <cassert>

#include "../operators/loaders/table.h"


using namespace std;



Schema s;


string formatout(const vector<string>& str) {
	string ret = "|";
	for (vector<string>::const_iterator it=str.begin(); it!=str.end(); ++it)
		ret += *it + '|';
	return ret;
}




void* readfromsharedmem(const char* shmname, size_t length_to_map)
{
	assert(length_to_map != 0);

	// Create shared mem segment.
	
	int fd;
	void* file_memory;

	fd = open(shmname, O_RDONLY, S_IRUSR);
	//fd = shm_open(shmname, O_RDWR | O_EXCL | O_CREAT, S_IRUSR | S_IWUSR);
	assert(fd != -1);
	//length_to_map = 1048576;

	cout << length_to_map << endl;
	lseek(fd, length_to_map-1, SEEK_SET);
	write(fd, "", 1);
	// Map memory.
	// MAP_SHARED makes the updates visible to the outside world.
	// MAP_NORESERVE says not to preserve swap space.
	// MAP_LOCKED hits default 32K limit and fails.
	//
	file_memory = mmap(NULL, length_to_map, PROT_READ, 
			MAP_SHARED | MAP_NORESERVE /* | MAP_LOCKED */, fd, 0);
	//struct Memory *shm_ptr =  (struct Memory*)file_memory;
	printf ("shared memory for pythia attached at address %p\n", file_memory);
	cout << sizeof(file_memory) << endl;
	if (file_memory == MAP_FAILED)
	{
		perror("mmap()");
	}
	
	// At this point we have the region in our virtual address space, but it
	// points to an empty file and thus should not be touched.
	// *(unsigned long long*)memory = 42ull; //< would fail with SIGBUS

	// Truncate empty file to extend it to appropriate size. 
	// Now writes will go through.

	munmap (file_memory, length_to_map);
	close(fd);
	int fc = unlink(shmname);
	//cout << fc << endl;

}

void cleanup(const char* shmname)
{
	unlink(shmname);
}

void testload(const char* shmname)
{
	

	MemMappedTable table;
	table.init(&s);

	if (table.load(shmname, "", Table::SilentLoad, Table::PermuteFiles) != MemMappedTable::LOAD_OK)
	{
		table.close();
		cleanup(shmname);
		printf("Load failed");
	}

	TupleBuffer* bf;
	void* tuple;
	while ( (bf = table.readNext()) )
	{
		TupleBuffer::Iterator it = bf->createIterator();
		while ( (tuple = it.next()) )
		{
			cout << formatout(table.schema()->outputTuple(tuple)) << '\n';
		}	

	}

	table.close();

	cleanup(shmname);
}

int main ()
{
	//readschema("outputschema");
	s.add(CT_LONG);
	s.add(CT_LONG);
	s.add(CT_DECIMAL);
	readfromsharedmem("/pythia-output", 1048576);
	testload("/pythia-output");
	return 0;
}
