/*************************************************************************
	> File Name: def.h
	> Author: lihy
	> Mail: lihaiyangcsu@gmail.com 
	> Created Time: 2015-07-27
 ************************************************************************/

#ifndef _DEF_H
#define _DEF_H

/*data type*/
typedef unsigned int offset_t;
typedef unsigned int uint32_t;
typedef signed int int32_t;

struct VAR_BUF_T
{
	uint32_t length;
	uint32_t size;
	void* buf;
};

/*core data type definitions*/
struct CATALOG_INFO_T
{	
	uint32_t depth_and_count;	/*[0...n]count of entry,[n+1...31]locale depth of bucket*/
	int32_t entry_first;		/*chaining to solve a collision*/
};

struct ENTRY_INFO_T
{
	uint32_t hash_code;			/*order by hash_code in one bucket*/ 
	uint32_t data_offset;
	uint32_t key_value_size;	/*[0...n]value,[n+1...31]key*/
	int32_t entry_next;
};

struct FILE_INFO_T
{
	#define SEA_CACHED_FILENAME_MAX (255)
	char name[SEA_CACHED_FILENAME_MAX];
	union COUNTER_T
	{
		uint32_t cursor;	
		uint32_t count;	/*current total entities or current total catalogs */
	}counter;
	uint32_t length;
	uint32_t align_size;
};

struct HEADER_INFO_T
{
	#define SEA_CACHED_TABLENAME_MAX (32)
	char table_name[SEA_CACHED_TABLENAME_MAX];
	uint32_t bucket_size;	/*bucket average size*/	
	uint32_t flag;			/*compression;hash function type;bloom filter;align*/
	uint32_t version;	
	uint32_t catalog_depth;	/*global depth*/
	uint32_t catalog_total;	
	uint32_t entry_index;
	uint32_t entry_total;

	struct FILE_INFO_T catalog;
	struct FILE_INFO_T entry;
	struct FILE_INFO_T data;
};

struct MMAP_INFO_T
{
	void *base;					/*the returned addr when invoke mmap()*/
	int32_t fd;					/*invoke open()*/
};

struct HASH_TABLE_T
{
	struct HEADER_INFO_T *header;	
	struct MMAP_INFO_T catalog;
	struct MMAP_INFO_T entry;
	struct MMAP_INFO_T data;	
	struct HASH_TABLE_T *next;
};

struct SEA_CACHED_T
{
	struct FILE_INFO_T *file;
	struct MMAP_INFO_T *mmap;
	struct HASH_TABLE_T *hash_table;
};

#define SEA_CACHED_OK				((int32_t) 0)
#define SEA_CACHED_ERROR			((int32_t)-1)

#define SEA_CACHED_VERSION			((uint32_t)1)
#define SEA_CACHED_BUCKET_SIZE		((int32_t) 8)	/*default 8*/
#define SEA_CACHED_CATALOG_MULTIPLE	((int32_t) 1)	/*default 8*/

#define SEA_CACHED_SET_CALL			((int32_t)0x00000001)
#define SEA_CACHED_GET_CALL			((int32_t)0x00000002)
#define SEA_CACHED_DELETE_CALL		((int32_t)0x00000004)
#define SEA_CACHED_UPDATE_CALL		((int32_t)0x00000008)

#define SEA_CACHED_KEY_EXIST		((int32_t)0x00000001)
#define SEA_CACHED_KEY_NON_EXIST	((int32_t)0x00000002)

#define SEA_CACHED_HASHTABLE_UNUSED ((int32_t)0x00000000)
#define SEA_CACHED_HASHTABLE_USED	((int32_t)0x00000001)

#define SEA_CACHED_CATALOG_SUFFIX	(".catalog")
#define SEA_CACHED_ENTRY_SUFFIX		(".entry")
#define SEA_CACHED_DATA_SUFFIX		(".data")

#define DEBUG(...)\
printf("[DEBUG] [%d] [%s:%d] ", getpid(), __FILE__, __LINE__ );\
printf( __VA_ARGS__ );
#endif

