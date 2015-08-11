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
	uint32_t entry_count		:8;		/*[0...n]count of entry*/
	uint32_t hash_collision		:8;	
	uint32_t bucket_depth		:16;	/*[n+1...31]locale depth of bucket*/
	int32_t	 entry_first;				/*chaining to solve a collision*/
};

struct ENTRY_INFO_T
{
	uint32_t hash_code;			/*order by hash_code in one bucket*/ 
	uint32_t data_offset;
	uint32_t value_size	:22;	/*[0...n]value>*/
	uint32_t key_size	:10;	/*[n+1...31]key*/
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
	uint32_t depth_max;

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
	int32_t flag;
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
#define SEA_CACHED_DEPTH_MAXIMUM	((uint32_t)2)
#define SEA_CACHED_BUCKET_SIZE		((int32_t) 4)	/*default 8*/

#define SEA_CACHED_KEY_EXIST		((int32_t)0x00000001)
#define SEA_CACHED_KEY_NON_EXIST	((int32_t)0x00000002)

#define SEA_CACHED_HASHTABLE_UNUSED ((int32_t)0x00000000)
#define SEA_CACHED_HASHTABLE_USED	((int32_t)0x00000001)

#define SEA_CACHED_HASHTABLE_CLOSE	((int32_t)0x00000000)
#define SEA_CACHED_HASHTABLE_OPEN	((int32_t)0x00000001)

#define SEA_CACHED_CATALOG_SUFFIX	(".catalog")
#define SEA_CACHED_ENTRY_SUFFIX		(".entry")
#define SEA_CACHED_DATA_SUFFIX		(".data")

#define SEA_CACHED_LOG_DEBUG		((int32_t)0x00000001)
#define SEA_CACHED_LOG_INFO			((int32_t)0x00000002)
#define SEA_CACHED_LOG_WARN			((int32_t)0x00000004)
#define SEA_CACHED_LOG_ERROR		((int32_t)0x00000008)
#define SEA_CACHED_LOG_FATAL		((int32_t)0x000000016)

#define DEBUG(...)\
printf("[DEBUG] [%d] [%s:%d] ", getpid(), __FILE__, __LINE__ );\
printf( __VA_ARGS__ );


/*functions declaration*/

int32_t 
hash_table_set( struct HASH_TABLE_T *hash_table, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value, 
				uint32_t (*hash_callback)(const struct VAR_BUF_T *key), 
				int32_t (*compression_callback)( const struct VAR_BUF_T *key, struct VAR_BUF_T *res) );

int32_t 
hash_table_get( const struct HASH_TABLE_T *hash_table, const struct VAR_BUF_T *key, struct VAR_BUF_T *value,
				uint32_t (*hash_callback)(const struct VAR_BUF_T *key), 
				int32_t (*uncompression_callback)( const struct VAR_BUF_T *key, struct VAR_BUF_T *res) );

int32_t 
sea_cached_hash_table_close( const struct SEA_CACHED_T *cached, struct HASH_TABLE_T *hash_table );

struct HASH_TABLE_T*
sea_cached_hash_table_create( struct SEA_CACHED_T *cached, const char *table_name, uint32_t bucket_size );

int32_t 
sea_cached_hash_table_initial( struct HASH_TABLE_T *hash_table );

struct HASH_TABLE_T*
sea_cached_hash_table_seach( struct HASH_TABLE_T *hash_table, const char *table_name, int32_t size );

int32_t 
sea_cached_initial( struct SEA_CACHED_T *cached );

int32_t 
sea_cached_close( struct SEA_CACHED_T *cached );

/*functions declaration*/

#endif

