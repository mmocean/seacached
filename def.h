/*************************************************************************
	> File Name: def.h
	> Author: lihy
	> Mail: lihaiyangcsu@gmail.com 
	> Created Time: 2015-07-27
 ************************************************************************/

#ifndef _DEF_H
#define _DEF_H

#define SEA_CACHED_VERSION (1)

#define BLOOM_FILTER_SIZE_MAX	(0x100000)	//default 1MB (1<<20)

#define ENTITY_SIZE_MAX			(0x100000)				//default 1MB (1<<20)
#define ENTITY_KEY_LENGTH_MAX	(0x400)			//default 1024B (1<<10)
#define BUCKET_AVERAGE_SIZE_MAX (0x10)				//default 16 (1<<4) to count bucket number


//data type
typedef unsigned int offset_t;
typedef unsigned int uint32_t;
typedef signed int int32_t;

//core data type definitions
struct HEADER
{
	int32_t version;
	int32_t filter_size;
	int32_t bucket_number;	//fixed, max bucket
	int32_t bucket_size;	//bucket average size
	int32_t entity_number;	//fixed, max entity
	int32_t entity_count;	//current total entities
	int32_t content_size;

	offset_t filter_offset;//bloom filter
	offset_t bucket_offset;
	offset_t entity_offset;
	offset_t content_offset;
};


struct BUCKET
{
	int32_t first_entity;//chaining to solve a collision
};


struct ENTITY
{
	int32_t kv_size;	//[0...n]value,[n+1...31]key
	uint32_t hash_code;	//order by hash_code in one bucket 
	offset_t content_index;
	int32_t entity_next;
};


struct SEACACHED
{
	struct HEADER* header;
	int32_t fd;
	offset_t content;	
	uint32_t file_length;

	void* mmap_base;
	void* filter_base;//bloom filter
	void* bucket_base;
	void* entity_base;
	void* content_base;
};


/*
//data size
#define HEADER_SIZE (sizeof(HEADER))
#define BUCKET_SIZE (sizeof(BUCKET))
#define ENTITY_SIZE (sizeof(ENTITY))
*/


#endif
