/*************************************************************************
	> File Name: seacached.c
	> Author: lihy
	> Mail: lihaiyangcsu@gmail.com 
	> Created Time: 2015-07-28
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>		//malloc free
#include <string.h>		//strerror
#include <assert.h>		//assert
#include <sys/mman.h>	//madvise mmap munmap msync
#include <unistd.h>		//lseek close write sysconf read

#include <sys/stat.h>	//fstat
#include <fcntl.h>		//O_RDWR O_CREAT S_IRWXU open

#include <errno.h>		//errno
#include <sys/time.h>	//gettimeofday
#include "def.h"

static int32_t 
sea_cached_file_extension( struct FILE_INFO_T *file, struct MMAP_INFO_T *map );

static int32_t 
sea_cached_file_mmap( struct FILE_INFO_T *file, struct MMAP_INFO_T *map );

static int32_t 
hash_table_data_write( struct HASH_TABLE_T *hash_table, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value, int32_t (*compression_callback)( const struct VAR_BUF_T *key, struct VAR_BUF_T *res) );

static int32_t 
hash_table_data_compression( const struct VAR_BUF_T *key, struct VAR_BUF_T *res )
{
	assert( NULL != key && NULL != key->buf );
	assert( NULL != res && NULL != res->buf );
	
	DEBUG( "hash_table_data_compression:\n" );

	return SEA_CACHED_OK;
}

static uint32_t 
hash_calculating( const struct VAR_BUF_T *key )
{
	assert( NULL != key && NULL != key->buf );
	
	DEBUG( "hash_calculating:\n" );

	//BKDRHash
	//magic number
	uint32_t seed = 131; 

	const char *str = (const char *)key->buf;	
	uint32_t hash_code = 0;
	uint32_t i = 0;
	for( i = 0; i<key->size; ++i )
	{
		hash_code = hash_code*seed + (*str++);
	}

	return hash_code&0x7fffffff;
}


static void
dump_header_info( struct HEADER_INFO_T *header )
{
	assert( NULL != header );
	
	char buf[512];
	memset( buf, 0, sizeof(buf) );
	int32_t size = 0;

	size += snprintf( buf, sizeof(buf), 
		"%s: %s\n %s: %u\n %s: %u\n %s: %u\n "
		"%s: %u\n %s: %u\n %s: %u\n %s: %u\n ",
		"# table_name #", header->table_name,
		"flag", header->flag,
		"version", header->version,
		"bucket_size", header->bucket_size,
		"catalog_depth", header->catalog_depth,	
		"catalog_total", header->catalog_total,
		"entry_index", header->entry_index,
		"entry_total", header->entry_total );

		//"catalog_count", header->catalog_count,
		//"entry_count", header->entry_count,	
	
	size += snprintf( buf+size, sizeof(buf)-size, 
		"%s\n %s: %s\n %s: %u\n %s: %u B\n %s: %u B\n ",
		"# catalog #",
		"name", header->catalog.name,
		"count", header->catalog.counter.count,
		"length", header->catalog.length,
		"align_size", header->catalog.align_size );
	size += snprintf( buf+size, sizeof(buf)-size, 
		"%s\n %s: %s\n %s: %u\n %s: %u B\n %s: %u B\n ",
		"# entry #",
		"name", header->entry.name,
		"count", header->entry.counter.count,
		"length", header->entry.length,
		"align_size", header->entry.align_size );
	size += snprintf( buf+size, sizeof(buf)-size, 
		"%s\n %s: %s\n %s: %u\n %s: %u B\n %s: %u B\n ",
		"# data #",
		"name", header->data.name,
		"cursor", header->data.counter.cursor,
		"length", header->data.length,
		"align_size", header->data.align_size );

	DEBUG( "dump_header_info:\n %s\n", buf );
}


/*
static void
offset_counting( struct HEADER *header )
{
	assert( NULL != header );
	
	header->filter_offset = sizeof( struct HEADER );
	header->bucket_offset = header->filter_offset + header->filter_size;
	header->entity_offset = header->bucket_offset + sizeof(struct BUCKET) * header->bucket_number;
	header->content_offset = header->entity_offset + sizeof(struct ENTITY) * header->entity_number;

	int32_t mod = header->content_offset%header->page_size;
	header->file_length = ( 0 == mod )?(header->content_offset):( (header->content_offset/header->page_size+1)*header->page_size );

}

static void
base_counting( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );
	
	const struct HEADER* header = cached->header;	

	const void* mmap_base = cached->mmap_base;
	cached->filter_base = (void*)( (char*)mmap_base+header->filter_offset );
	cached->bucket_base = (void*)( (char*)mmap_base+header->bucket_offset );
	cached->entity_base = (void*)( (char*)mmap_base+header->entity_offset );
	cached->content_base = (void*)( (char*)mmap_base+header->content_offset );
}


static int32_t 
header_initial( struct HEADER *header )
{
	assert( NULL != header );
	
	header->version = SEA_CACHED_VERSION;
	header->entity_number = ENTITY_SIZE_MAX;
	header->bucket_size = BUCKET_AVERAGE_SIZE_MAX;
	header->filter_size = sizeof(int32_t) * BLOOM_FILTER_SIZE_MAX;
	
	header->bucket_number = bucket_number_counting( header->entity_number, header->bucket_size );
	if( SEA_CACHED_ERROR == header->bucket_number )
	{
		DEBUG( "bucket_number_counting error\n" );
		return SEA_CACHED_ERROR;
	}

	header->page_size = sysconf(_SC_PAGESIZE);
	if( -1 == header->page_size )
	{
		DEBUG( "sysconf _SC_PAGESIZE error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}

	offset_counting( header );
	
	return SEA_CACHED_OK;
}
*/

static int32_t 
sea_cached_initial( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );
	
	DEBUG( "sea_cached_initial:\n" );	
	
	struct FILE_INFO_T *file = cached->file;
	struct MMAP_INFO_T *mmap = cached->mmap;

	if( SEA_CACHED_OK != sea_cached_file_mmap( file, mmap ) )
	{
		DEBUG( "sea_cached_file_mmap error\n" );
	}

	cached->hash_table = NULL;

	int32_t table_number = file->length/sizeof(struct HEADER_INFO_T);	
	int i = 0 ;
	for( i = 0; i<table_number; ++i )
	{		
		struct HASH_TABLE_T *tmp = (struct HASH_TABLE_T*)malloc(sizeof(struct HASH_TABLE_T));
		if( NULL == tmp )
			return SEA_CACHED_ERROR;	
		memset( tmp, 0, sizeof(struct HASH_TABLE_T) );
		tmp->header = (struct HEADER_INFO_T *)mmap->base + i;
		if( NULL == cached->hash_table )
		{
			tmp->next = NULL;
			cached->hash_table = tmp;
		} else {
			tmp->next = cached->hash_table;
			cached->hash_table = tmp;		
		}
	}

	return SEA_CACHED_OK;
}


static struct HASH_TABLE_T*
sea_cached_hash_table_seach( struct HASH_TABLE_T *hash_table, const char *table_name, int32_t size )
{
	assert( NULL != table_name );

	DEBUG( "sea_cached_hash_table_seach:\n" );

	while( NULL != hash_table )
	{
		struct HEADER_INFO_T *header = (struct HEADER_INFO_T *)hash_table->header;
		if( 0 == strncmp( table_name, header->table_name, size ) )
			return hash_table;
		hash_table = hash_table->next;
	}

	return NULL;
}


static struct HASH_TABLE_T*
hash_table_create( struct SEA_CACHED_T *cached, const char *table_name, uint32_t bucket_size )
{
	assert( NULL != cached && NULL != cached->file && NULL != cached->mmap && NULL != table_name );
	
	DEBUG( "hash_table_create:\n" );

	struct HASH_TABLE_T *hash_table = cached->hash_table;
	while( NULL != hash_table )
	{
		struct HEADER_INFO_T *header = (struct HEADER_INFO_T *)hash_table->header;
		if( SEA_CACHED_HASHTABLE_UNUSED == (header->flag|SEA_CACHED_HASHTABLE_UNUSED) )
			break;
		hash_table = hash_table->next;
	}

	struct HEADER_INFO_T *header = NULL;
	if( NULL == hash_table )
	{
		if( SEA_CACHED_OK != sea_cached_file_extension( cached->file, cached->mmap ) )
		{
			DEBUG( "sea_cached_file_extension error\n" );
			return NULL;
		}
		header = (struct HEADER_INFO_T *)cached->mmap->base + (cached->file->length/cached->file->align_size-1);
	} else {
		header = (struct HEADER_INFO_T *)hash_table->header;
	}

	memset( header, 0, sizeof(struct HEADER_INFO_T) );		
	strncpy( header->table_name, table_name, sizeof(header->table_name) );
	header->bucket_size = bucket_size;
	header->version	= SEA_CACHED_VERSION;	

	struct FILE_INFO_T *catalog = &header->catalog;
	struct FILE_INFO_T *entry = &header->entry;
	struct FILE_INFO_T *data = &header->data;	
	
	snprintf( catalog->name, sizeof(catalog->name), "%s%s", table_name, SEA_CACHED_CATALOG_SUFFIX );
	snprintf( entry->name, sizeof(entry->name), "%s%s", table_name, SEA_CACHED_ENTRY_SUFFIX );
	snprintf( data->name, sizeof(data->name), "%s%s", table_name, SEA_CACHED_DATA_SUFFIX );

	catalog->align_size = entry->align_size = data->align_size = sysconf(_SC_PAGESIZE);

	struct HASH_TABLE_T *tmp = (struct HASH_TABLE_T*)malloc(sizeof(struct HASH_TABLE_T));
	if( NULL == tmp )
		return NULL;
	memset( tmp, 0, sizeof(struct HASH_TABLE_T) );
	tmp->header = header;

	if( NULL == cached->hash_table )
	{
		tmp->next = NULL;
		cached->hash_table = tmp;
	} else {
		tmp->next = cached->hash_table;
		cached->hash_table = tmp;		
	}		

	return tmp;
}

static int32_t 
hash_table_initial( struct HASH_TABLE_T *hash_table )
{
	assert( NULL != hash_table && NULL != hash_table->header );
	
	DEBUG( "hash_table_initial:\n" );
	
	struct HEADER_INFO_T *header = hash_table->header;

	struct FILE_INFO_T *catalog = &header->catalog;
	struct FILE_INFO_T *entry = &header->entry;
	struct FILE_INFO_T *data = &header->data;

	if( SEA_CACHED_OK != sea_cached_file_mmap( catalog, &hash_table->catalog ) )
	{
		DEBUG( "sea_cached_file_mmap error\n" );
		return SEA_CACHED_ERROR;
	}
	header->catalog_total = catalog->length/sizeof(struct CATALOG_INFO_T);
	header->catalog.counter.count = 2;	//default

	if( SEA_CACHED_OK != sea_cached_file_mmap( entry, &hash_table->entry ) )
	{
		DEBUG( "sea_cached_file_mmap error\n" );
		return SEA_CACHED_ERROR;
	}
	header->entry_total = entry->length/sizeof(struct ENTRY_INFO_T);
	
	if( SEA_CACHED_OK != sea_cached_file_mmap( data, &hash_table->data ) )
	{
		DEBUG( "sea_cached_file_mmap error\n" );
		return SEA_CACHED_ERROR;
	}

	header->flag |= SEA_CACHED_HASHTABLE_USED;

	return SEA_CACHED_OK;
}


/*
 * exists:SEA_CACHED_OK is returned, copy content to value 
 * otherwise:SEA_CACHED_ERROR is returned, copy previous entity to 
 * */


static int32_t
hash_table_search( const struct HASH_TABLE_T *hash_table, const struct VAR_BUF_T *key, 
				uint32_t hash_code, int32_t entry_first, const struct ENTRY_INFO_T **entry )
{
	assert( NULL != hash_table && NULL != entry );
	assert( NULL != key && NULL != key->buf );

	DEBUG( "hash_table_search:\n" );	

	const struct MMAP_INFO_T *mmap_entry = &hash_table->entry;
	struct ENTRY_INFO_T *pre = NULL;
	int32_t entry_next = entry_first; 
	while( -1 != entry_next )
	{	
		struct ENTRY_INFO_T *iter = (struct ENTRY_INFO_T *)mmap_entry->base + entry_next;
	
		if( hash_code < iter->hash_code )
			break;

		if( hash_code == iter->hash_code && ((iter->key_value_size>>22)&0x3ff) == key->size )
		{
			const struct MMAP_INFO_T *data = &hash_table->data;
			if( 0 == memcmp( (void*)((char*)data->base+iter->data_offset), (void*)key->buf, key->size ) )
			{
				DEBUG( "key already exists\n" );
				*entry = iter;						
				return SEA_CACHED_KEY_EXIST;
			}
		}	
		pre = iter;
		entry_next = iter->entry_next;	
	}

	*entry = pre;							
	DEBUG( "key does not exist\n" );

	return SEA_CACHED_KEY_NON_EXIST;	
}


static int32_t 
available_entry_search( struct HEADER_INFO_T *header, struct MMAP_INFO_T *mmap_entry )
{
	assert( NULL != header && NULL != mmap_entry );

	int32_t entry_index = -1;
	if( header->entry_index >= header->entry_total )
	{
		//search in the free list of entry
		//to do

		//entry zone extension
		if( SEA_CACHED_OK != sea_cached_file_extension( &header->entry, mmap_entry ) )
		{
			DEBUG( "sea_cached_file_extension error\n" );
			return SEA_CACHED_ERROR;
		}
		header->entry_total = header->entry.length/sizeof(struct ENTRY_INFO_T);
		entry_index = header->entry_index;
	} else {
		//direct allocation
		entry_index = header->entry_index;
	}

	DEBUG( "available entry index:%d\n", entry_index );

	return entry_index;
}


static int32_t 
hash_table_catalog_multiplier( struct HEADER_INFO_T *header, struct MMAP_INFO_T *mmap_catalog, uint32_t catalog_index )
{
	assert( NULL != header && NULL != mmap_catalog );

	DEBUG( "hash_table_catalog_multiplier:\n" );

	//catalog zone extension
	while( (header->catalog.counter.count<<1) > header->catalog_total )
	{
		if( SEA_CACHED_OK != sea_cached_file_extension( &header->catalog, mmap_catalog ) )
		{
			DEBUG( "sea_cached_file_extension error\n" );
			return SEA_CACHED_ERROR;
		}
		header->catalog_total = header->catalog.length/sizeof(struct CATALOG_INFO_T);
	}

	//catalog multiplier 

	return SEA_CACHED_OK;
}


static int32_t 
hash_table_set( struct HASH_TABLE_T *hash_table, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value, 
				uint32_t (*hash_callback)(const struct VAR_BUF_T *key), 
				int32_t (*compression_callback)( const struct VAR_BUF_T *key, struct VAR_BUF_T *res) )
{
	assert( NULL != hash_table && NULL != hash_table->header );
	assert( NULL != key && NULL != key->buf );
	assert( NULL != value );
	
	DEBUG( "hash_table_set:\n" );

	struct HEADER_INFO_T *header = hash_table->header;

	dump_header_info( header );

	//hash_counting
	uint32_t hash_code = (NULL == hash_callback)?hash_calculating( key ):hash_callback( key );
	DEBUG( "hash_code:%u\n", hash_code );

	uint32_t catalog_depth = header->catalog_depth;
	uint32_t catalog_index = hash_code&(1<<catalog_depth);		
	DEBUG( "catalog_depth:%d catalog_index:%d\n", catalog_depth, catalog_index );
	
	struct MMAP_INFO_T *mmap_catalog = &hash_table->catalog;
	struct CATALOG_INFO_T *cl = (struct CATALOG_INFO_T *)mmap_catalog->base+catalog_index;

	uint32_t bucket_depth = (cl->depth_and_count>>16)&0xffff;
	uint32_t bucket_count = cl->depth_and_count&0xffff;
	DEBUG( "bucket_depth:%d bucket_count:%d\n", bucket_depth, bucket_count );

	int32_t entry_first = cl->entry_first;
	DEBUG( "entry_first:%d\n", entry_first );
		
	const struct ENTRY_INFO_T *pre = NULL;
	if( 0 != bucket_count )
	{
		int32_t ret = hash_table_search( hash_table, key, hash_code, entry_first, &pre );
		if( SEA_CACHED_KEY_EXIST == ret )
		{
			DEBUG( "duplicate key\n" );	
			return SEA_CACHED_ERROR;	
		}
	}

	int32_t catalog_multiplier_flag = 0;
	if( bucket_count == header->bucket_size )
	{
		if( SEA_CACHED_OK != hash_table_catalog_multiplier( header, mmap_catalog, catalog_index ) )
		{
			DEBUG( "hash_table_catalog_multiplier error\n" );	
			return SEA_CACHED_ERROR;
		}
		catalog_multiplier_flag = SEA_CACHED_CATALOG_MULTIPLE;
	}

	//search and insert if unique
	struct MMAP_INFO_T *mmap_entry = &hash_table->entry;
	int32_t entry_index = available_entry_search( header, mmap_entry );
	if( SEA_CACHED_ERROR == entry_index )
	{
		DEBUG( "available_entry_search error\n" );
		return SEA_CACHED_ERROR;
	}

	//write key-value to data zone
	if( SEA_CACHED_OK != hash_table_data_write( hash_table, key, value, compression_callback ) )
	{
		DEBUG( "hash_table_data_write error\n" );	
		return SEA_CACHED_ERROR;
	}

	struct ENTRY_INFO_T *new_entry = (struct ENTRY_INFO_T *)mmap_entry->base+entry_index;
	new_entry->hash_code = hash_code;
	new_entry->key_value_size = 0;
	new_entry->key_value_size |= (key->size<<22);
	new_entry->key_value_size |= (value->size);
	new_entry->data_offset = header->data.counter.cursor;

	//insert entry to bucket chain
	if( 0 == bucket_count || NULL == pre )
	{
		cl->entry_first = entry_index;
		new_entry->entry_next = -1;
	} else {
		struct ENTRY_INFO_T *tmp = (struct ENTRY_INFO_T *)pre;
		new_entry->entry_next = tmp->entry_next;
		tmp->entry_next = entry_index;	
	}	

	header->entry.counter.count += 1;

	if( header->entry_index < header->entry_total )
		header->entry_index += 1;

	header->data.counter.cursor += (key->size+value->size);
	
	cl->depth_and_count = (cl->depth_and_count&0xffff)+1;

	if( SEA_CACHED_CATALOG_MULTIPLE == catalog_multiplier_flag )
		header->catalog_depth = ( header->catalog_depth >= (bucket_depth+1) )?header->catalog_depth:(bucket_depth+1);

	//struct MMAP_INFO_T *mmap_data = &hash_table->data;
	
	DEBUG( "hash_table_set OK\n" );
	
	return SEA_CACHED_OK;
}

/*
static int32_t 
hash_table_get( const struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, struct VAR_BUF_T *value )
{
	assert( NULL != cached && NULL != cached->header );
	assert( NULL != key && NULL != key->buf );
	assert( NULL != value );
	
	DEBUG( "hash_table_get:\n" );
	
	struct HEADER *header = cached->header;
	
	//hash_counting
	uint32_t hash_code = hash_calculating( key );
	DEBUG( "hash_code:%u\n", hash_code );

	int32_t bucket_index = hash_code%header->bucket_number;
	DEBUG( "bucket_index:%d\n", bucket_index );

	struct BUCKET *bucket_base = (struct BUCKET*)( cached->bucket_base );
	int32_t entity_number = (bucket_base+bucket_index)->entity_number;
	DEBUG( "entity_number of bucket:%d\n", entity_number );
	
	struct ENTITY *entity = NULL;
	if( 0 != entity_number )
	{
		int32_t call_flag = SEA_CACHED_GET_CALL; 
		int32_t ret = hash_table_search( cached, key, hash_code, entity_number, bucket_index, 0, call_flag, &entity );
		if( SEA_CACHED_KEY_NON_EXIST == ret )
		{
			DEBUG( "nonexistence key\n" );
			return SEA_CACHED_KEY_NON_EXIST;	
		}
	} else {
		DEBUG( "nonexistence key\n" );
		return SEA_CACHED_KEY_NON_EXIST;	
	}

	value->size = entity->kv_size&0x3fffff;
	DEBUG( "value size:%d\n", value->size );
	if( value->size > value->length )
	{
		//reallocate
		if( NULL != value->buf )
			free( (void*)value->buf );
		value->buf = (void*)malloc( value->size );
		if( NULL == value->buf )
		{
			DEBUG( "malloc error\n" );
			return SEA_CACHED_ERROR;
		}
		value->length = value->size;
	}

	memcpy( (void*)value->buf, (void*)( (char*)cached->mmap_base+entity->content_index+((entity->kv_size>>22)&0x3ff) ), value->size );

	DEBUG( "hash_table_get OK\n" );

	return SEA_CACHED_OK;
}

static int32_t 
sea_cached_delete( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key )
{
	assert( NULL != cached && NULL != cached->header );
	assert( NULL != key && NULL != key->buf );
	
	DEBUG( "sea_cached_delete:\n" );
	
	struct HEADER *header = cached->header;
	
	//hash_counting
	uint32_t hash_code = hash_calculating( key );
	DEBUG( "hash_code:%u\n", hash_code );

	int32_t bucket_index = hash_code%header->bucket_number;
	DEBUG( "bucket_index:%d\n", bucket_index );

	struct BUCKET *bucket_base = (struct BUCKET*)( cached->bucket_base );
	int32_t entity_number = (bucket_base+bucket_index)->entity_number;
	DEBUG( "entity_number of bucket:%d\n", entity_number );
	
	struct ENTITY *entity = NULL;
	if( 0 != entity_number )
	{
		int32_t call_flag = SEA_CACHED_DELETE_CALL; 
		int32_t ret = hash_table_search( cached, key, hash_code, entity_number, bucket_index, 0, call_flag, &entity );
		if( SEA_CACHED_KEY_NON_EXIST == ret )
		{
			DEBUG( "nonexistence key\n" );
			return SEA_CACHED_KEY_NON_EXIST;	
		}
	} else {
		DEBUG( "nonexistence key\n" );
		return SEA_CACHED_KEY_NON_EXIST;	
	}

	struct ENTITY *entity_base = (struct ENTITY*)( cached->entity_base );
	struct ENTITY *next = NULL;
	if( NULL == entity )
	{
		next = (entity_base + (bucket_base+bucket_index)->first_entity);
		(bucket_base+bucket_index)->first_entity = next->entity_next;
	} else {
		next = (entity_base + entity->entity_next);
		entity->entity_next = next->entity_next;
	}

	next->entity_next = -1;
	next->hash_code = 0;
	(bucket_base+bucket_index)->entity_number -= 1;

	header->entity_count -= 1;

	DEBUG( "sea_cached_delete OK\n" );
	return SEA_CACHED_OK;
}
*/

static int32_t 
sea_cached_file_extension( struct FILE_INFO_T *file, struct MMAP_INFO_T *map )
{
	assert( NULL != file && NULL != map );
		
	DEBUG( "sea_cached_file_extension:\n" );

	uint32_t length = (file->length/file->align_size+1)*file->align_size;

	if( -1 == lseek( map->fd, length-1, SEEK_SET ) )
	{
		DEBUG( "lseek error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}

	char nil = 0;
	int32_t size = write( map->fd, (void*)&nil, sizeof(char) );
	DEBUG( "write size:%d\n", size );
	if( -1 == size )
	{
		DEBUG( "write error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;	
	}

	off_t off = 0;
	void *base = (void*)mmap( NULL, length, PROT_READ|PROT_WRITE, MAP_SHARED, map->fd, off );
	if( MAP_FAILED == base )
	{
		DEBUG( "mmap error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}

	if( -1 == madvise( base, length, MADV_NORMAL ) )
	{
		DEBUG( "madvise error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}

	if( NULL != map->base )
	{	
		if ( -1 == msync( map->base, file->length, MS_SYNC ) )
		{
			DEBUG( "msync error:%s\n", strerror(errno) );		
			return SEA_CACHED_ERROR;
		}
		
		if( -1 == munmap( map->base, file->length ) )
		{
			DEBUG( "munmap error:%s\n", strerror(errno) );		
			return SEA_CACHED_ERROR;
		}
	}

	map->base = base;
	file->length = length;

	return SEA_CACHED_OK;
}

static int32_t 
sea_cached_file_mmap( struct FILE_INFO_T *file, struct MMAP_INFO_T *map )
{
	assert( NULL != file && NULL != map );

	DEBUG( "sea_cached_file_mmap:\n" );	

	//S_IRUSR  00400 user has read permission
	//S_IWUSR  00200 user has write permission
	int32_t fd = open( file->name, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR );
	if( -1 == fd )
	{
		DEBUG( "open error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}

	struct stat st;
	if( -1 == fstat( fd, &st ) )
	{
		DEBUG( "fstat error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;	
	}
	DEBUG( "filesize:%d\n", (int)st.st_size );

	if( 0 == st.st_size )
	{
		if( -1 == lseek( fd, file->align_size-1, SEEK_SET ) )
		{
			DEBUG( "lseek error:%s\n", strerror(errno) );
			return SEA_CACHED_ERROR;
		}

		char nil = 0;
		int32_t size = write( fd, (void*)&nil, sizeof(char) );
		DEBUG( "write size:%d\n", size );
		if( -1 == size )
		{
			DEBUG( "write error:%s\n", strerror(errno) );
			return SEA_CACHED_ERROR;	
		}
		file->length = file->align_size;
	} else {
		file->length = st.st_size;
	}


	off_t off = 0;
	void *base = (void*)mmap( NULL, file->length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, off );
	if( MAP_FAILED == base )
	{
		DEBUG( "mmap error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}

	if( -1 == madvise( base, file->length, MADV_NORMAL ) )
	{
		DEBUG( "madvise error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}

	map->base = base;
	map->fd = fd;

	return SEA_CACHED_OK;
}

/*
static int32_t 
sea_cached_file_close( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );

	struct HASH_TABLE_T *hash_table = cached->hash_table;
	while( NULL != hash_table )
	{
	
	
	}

	if( NULL != cached )
	{
		if ( SEA_CACHED_OK != sea_cached_file_sysnc( cached, MS_SYNC ) )
		{
			DEBUG( "sea_cached_file_sysnc error\n" );		
			return SEA_CACHED_ERROR;
		}
	
		if ( SEA_CACHED_OK != sea_cached_file_munmap( cached ) )
		{
			DEBUG( "sea_cached_file_munmap error\n" );		
			return SEA_CACHED_ERROR;
		}
	
		close( cached->fd );
		cached->fd = -1;

		cached->mmap_base = NULL;
		cached->filter_base = NULL;
		cached->bucket_base = NULL;
		cached->entity_base = NULL;
		cached->content_base = NULL;
	}

	return SEA_CACHED_OK;
}

*/

static int32_t 
hash_table_data_write( struct HASH_TABLE_T *hash_table, 
					const struct VAR_BUF_T *key, const struct VAR_BUF_T *value, 
					int32_t (*compression_callback)( const struct VAR_BUF_T *key, struct VAR_BUF_T *res) )
{
	assert( NULL != hash_table );
	assert( NULL != key && NULL != key->buf );
	assert( NULL != value );
	
	DEBUG( "hash_table_data_write:\n" );
	
	struct FILE_INFO_T *data = &hash_table->header->data;	
	
	DEBUG( "data->counter.cursor:%d data->length:%d\n",data->counter.cursor, data->length );
	DEBUG( "key->size:%d value->size:%d\n", key->size, value->size );
	
	while( (data->length-data->counter.cursor) < (key->size+value->size) )
	{
		//data zone extension
		if( SEA_CACHED_OK != sea_cached_file_extension( data, &hash_table->data ) )
		{
			DEBUG( "sea_cached_file_extension error\n" );
			return SEA_CACHED_ERROR;
		}
	}

	//to do
	if( 0 )
	{
		if( NULL != compression_callback )
			compression_callback( NULL, NULL );
		else 
			hash_table_data_compression( NULL, NULL );
	}

	struct MMAP_INFO_T *mmap_data = &hash_table->data;
	if( NULL != key->buf )
		memcpy( (void*)((char*)mmap_data->base+data->counter.cursor), (void*)key->buf, key->size );
	if( NULL != value->buf )
	memcpy( (void*)((char*)mmap_data->base+data->counter.cursor+key->size), (void*)value->buf, value->size );

	return SEA_CACHED_OK;
}


int main()
{
	//sea_cached_initial
	struct FILE_INFO_T file;
	memset( &file, 0, sizeof(struct FILE_INFO_T) );
	file.align_size = sizeof(struct HEADER_INFO_T);	
	strncpy( file.name, "./test/cache.cache", sizeof(file.name) );

	struct MMAP_INFO_T mmap;
	memset( &mmap, 0, sizeof(struct MMAP_INFO_T) );
	
	struct SEA_CACHED_T cached;
	memset( &cached, 0, sizeof(struct SEA_CACHED_T) );
	cached.file = (struct FILE_INFO_T*)&file;
	cached.mmap = (struct MMAP_INFO_T*)&mmap;

	if( SEA_CACHED_OK != sea_cached_initial( &cached ) )
	{
		DEBUG( "sea_cached_initial error\n" );
	}

	//hash_table_initial
	const char *table_name = "./test/test";
	struct HASH_TABLE_T *hash_table = sea_cached_hash_table_seach( cached.hash_table, table_name, strlen(table_name) );	
	uint32_t bucket_size = SEA_CACHED_BUCKET_SIZE;
	if ( NULL == hash_table && NULL == (hash_table = hash_table_create( &cached ,table_name, bucket_size ) ) )
	{
		DEBUG( "hash_table_create error\n" );
	}

	if( SEA_CACHED_OK != hash_table_initial( hash_table ) )
	{
		DEBUG( "hash_table_initial error\n" );
	}

	struct VAR_BUF_T key;
	struct VAR_BUF_T value;
	
	const char *buf1 = "1234568";
	key.buf = (void*)buf1;
	key.size = strlen(buf1);
	
	const char *buf2 = "ABCD";
	value.buf = (void*)buf2;
	value.size = strlen(buf2);

	struct timeval start;
	gettimeofday( &start, NULL );
	hash_table_set( hash_table, &key, &value, NULL, NULL );
	struct timeval stop;
	gettimeofday( &stop, NULL );

	DEBUG( "%d us\n", ((int32_t)stop.tv_sec-(int32_t)start.tv_sec)*1000000+(int32_t)stop.tv_usec-(int32_t)start.tv_usec );

	/*
	struct VAR_BUF_T res;
	memset( &res, 0, sizeof(res) );
	
	gettimeofday( &start, NULL );
	hash_table_get( &cached, &key, &res );
	gettimeofday( &stop, NULL );
	
	DEBUG( "%d %d \n", (int32_t)start.tv_sec, (int32_t)start.tv_usec );
	DEBUG( "%d %d \n", (int32_t)stop.tv_sec, (int32_t)stop.tv_usec );

	DEBUG( "value size:%d value:%s\n", res.size, (char*)res.buf );
	if( NULL != res.buf )
		free( (void*)res.buf );

	sea_cached_delete( &cached, &key );

	sea_cached_file_close( &cached );
	*/

	return 0;
}

