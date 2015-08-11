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

	
static int32_t 
hash_table_data_uncompression( const struct VAR_BUF_T *key, struct VAR_BUF_T *res )
{
	assert( NULL != key && NULL != key->buf );
	assert( NULL != res && NULL != res->buf );
	
	DEBUG( "hash_table_data_uncompression:\n" );

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
dump_header_info( const struct HEADER_INFO_T *header )
{
	assert( NULL != header );
	
	char buf[512];
	memset( buf, 0, sizeof(buf) );
	int32_t size = 0;

	size += snprintf( buf, sizeof(buf), 
		"%s: %s\n %s: %u\n %s: %u\n %s: %u\n "
		"%s: %u\n %s: %u\n %s: %u\n %s: %u\n %s: %u\n ",
		"# table_name #", header->table_name,
		"flag", header->flag,
		"version", header->version,
		"bucket_size", header->bucket_size,
		"catalog_depth", header->catalog_depth,	
		"catalog_total", header->catalog_total,
		"entry_index", header->entry_index,
		"entry_total", header->entry_total,
		"depth_max", header->depth_max );
	
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


static void
dump_catalog_info( const struct CATALOG_INFO_T *catalog )
{
	assert( NULL != catalog );
	
	char buf[512];
	memset( buf, 0, sizeof(buf) );
	int32_t size = 0;
	
	size += snprintf( buf, sizeof(buf), 
		"%s:\n %s: %u\n %s: %u\n %s: %u\n %s: %d\n ",
		"# catalog #",
		"entry_count", catalog->entry_count,
		"hash_collision", catalog->hash_collision,
		"bucket_depth", catalog->bucket_depth,
		"entry_first", catalog->entry_first );

	DEBUG( "dump_catalog_info:\n %s\n", buf );
}


static void
dump_entry_info( const struct ENTRY_INFO_T *entry )
{
	assert( NULL != entry );

	char buf[512];
	memset( buf, 0, sizeof(buf) );
	int32_t size = 0;
	
	size += snprintf( buf, sizeof(buf), 
		"%s:\n %s: %u\n %s: %u\n %s: %u\n %s: %u\n %s: %d\n ",
		"# entry #",
		"hash_code", entry->hash_code,
		"data_offset", entry->data_offset,
		"value_size", entry->value_size,
		"key_size", entry->key_size,
		"entry_next", entry->entry_next );

	DEBUG( "dump_entry_info:\n %s\n", buf );
}


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
sea_cached_hash_table_create( struct SEA_CACHED_T *cached, const char *table_name, uint32_t bucket_size )
{
	assert( NULL != cached && NULL != cached->file && NULL != cached->mmap && NULL != table_name );
	
	DEBUG( "sea_cached_hash_table_create:\n" );

	struct HASH_TABLE_T *hash_table = cached->hash_table;
	while( NULL != hash_table )
	{
		struct HEADER_INFO_T *header = (struct HEADER_INFO_T *)hash_table->header;
		if( SEA_CACHED_HASHTABLE_UNUSED == (header->flag&SEA_CACHED_HASHTABLE_UNUSED) )
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
sea_cached_hash_table_initial( struct HASH_TABLE_T *hash_table )
{
	assert( NULL != hash_table && NULL != hash_table->header );
	
	DEBUG( "sea_cached_hash_table_initial:\n" );

	if( SEA_CACHED_HASHTABLE_CLOSE != hash_table->flag )
	{
		DEBUG( "hash table has been opened\n" );
		return SEA_CACHED_OK;	
	}

	struct HEADER_INFO_T *header = hash_table->header;

	struct FILE_INFO_T *catalog = &header->catalog;
	struct FILE_INFO_T *entry = &header->entry;
	struct FILE_INFO_T *data = &header->data;

	if( SEA_CACHED_OK != sea_cached_file_mmap( catalog, &hash_table->catalog ) )
	{
		DEBUG( "sea_cached_file_mmap error\n" );
		return SEA_CACHED_ERROR;
	}

	if( 0 == header->catalog_total )
		header->catalog_total = catalog->length/sizeof(struct CATALOG_INFO_T);
	
	if( 0 == header->catalog.counter.count )
		header->catalog.counter.count = 1;	//default

	if( SEA_CACHED_OK != sea_cached_file_mmap( entry, &hash_table->entry ) )
	{
		DEBUG( "sea_cached_file_mmap error\n" );
		return SEA_CACHED_ERROR;
	}

	if( 0 == header->entry_total )
		header->entry_total = entry->length/sizeof(struct ENTRY_INFO_T);
	
	if( SEA_CACHED_OK != sea_cached_file_mmap( data, &hash_table->data ) )
	{
		DEBUG( "sea_cached_file_mmap error\n" );
		return SEA_CACHED_ERROR;
	}

	if( SEA_CACHED_HASHTABLE_UNUSED == (header->flag&SEA_CACHED_HASHTABLE_UNUSED) )
		header->flag |= SEA_CACHED_HASHTABLE_USED;

	if( 0 == header->depth_max )
		header->depth_max = SEA_CACHED_DEPTH_MAXIMUM;

	if( SEA_CACHED_HASHTABLE_CLOSE == hash_table->flag )
		hash_table->flag = SEA_CACHED_HASHTABLE_OPEN;

	return SEA_CACHED_OK;
}


/*
 * exists:SEA_CACHED_OK is returned, copy content to value 
 * otherwise:SEA_CACHED_ERROR is returned, copy previous entity to 
 * */

static int32_t
hash_table_search( const struct HASH_TABLE_T *hash_table, const struct VAR_BUF_T *key, 
				uint32_t hash_code, int32_t entry_first, const struct ENTRY_INFO_T **entry,
				uint32_t *hash_collison_count )
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

		if( hash_code == iter->hash_code && iter->key_size == key->size )
		{
			if( NULL != hash_collison_count )
				++(*hash_collison_count);
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

	return SEA_CACHED_OK;
}


static void
hash_table_adjust_bucket( struct MMAP_INFO_T *mmap_entry, struct CATALOG_INFO_T *catalog, struct ENTRY_INFO_T *entry, int32_t entry_index )
{
	assert( NULL != mmap_entry && NULL != catalog && NULL != entry );
	
	DEBUG( "hash_table_adjust_bucket:\n" );
	
	//insert into catalog	
	if( 0 == catalog->entry_count )
	{
		catalog->entry_first = entry_index;
		entry->entry_next = -1;
	} else {
		struct ENTRY_INFO_T *tmp = NULL;
		int32_t next = catalog->entry_first;
		uint32_t i = 0;
		for( i = 0; i<catalog->entry_count; ++i )
		{
			tmp = (struct ENTRY_INFO_T *)mmap_entry->base + next;
			if( entry->hash_code == tmp->hash_code )
			{												
				entry->entry_next = tmp->entry_next;
				tmp->entry_next = entry_index;
				catalog->hash_collision += 1;
				break;
			}
			if( i != (catalog->entry_count-1) )
				next = tmp->entry_next;
		}
		if( i == catalog->entry_count )
		{
			entry->entry_next = tmp->entry_next;
			tmp->entry_next = entry_index;
		}
	}
	catalog->entry_count += 1;
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

	struct MMAP_INFO_T *mmap_catalog = &hash_table->catalog;
	struct MMAP_INFO_T *mmap_entry = &hash_table->entry;
	
	struct CATALOG_INFO_T *cl = NULL;
	const struct ENTRY_INFO_T *pre = NULL;

	int32_t entry_first = 0;
	uint32_t entry_count = 0;
	uint32_t hash_collison_count = 0;
	while(1)
	{
		uint32_t catalog_depth = header->catalog_depth;
		uint32_t catalog_index = (0==catalog_depth)?0:hash_code&(1<<(catalog_depth-1));		
		DEBUG( "catalog_depth:%d catalog_index:%d\n", catalog_depth, catalog_index );

		cl = (struct CATALOG_INFO_T *)mmap_catalog->base+catalog_index;

		uint32_t bucket_depth = cl->bucket_depth;
		uint32_t hash_collision = cl->hash_collision;
		entry_count = cl->entry_count;
		DEBUG( "bucket_depth:%d entry_count:%d hash_collision:%d\n", bucket_depth, entry_count, hash_collision );

		entry_first = cl->entry_first;
		DEBUG( "entry_first:%d\n", entry_first );

		hash_collison_count = 0;
		pre = NULL;
		if( 0 != entry_count )
		{
			int32_t ret = hash_table_search( hash_table, key, hash_code, entry_first, &pre, &hash_collison_count );
			if( SEA_CACHED_KEY_EXIST == ret )
			{
				DEBUG( "duplicate key\n" );	
				return SEA_CACHED_ERROR;	
			}
		}

		/* if key1 != key2, but HASH(key1) == HASH(key2), so we should exclude duplicate hashcode
		 *
		 * */
		DEBUG( "hash_collison_count:%d\n", hash_collison_count );
		if( 0 == hash_collison_count && (entry_count-hash_collision) == header->bucket_size && bucket_depth != header->depth_max )
		{
			uint32_t catalog_mirror = catalog_index|(1<<bucket_depth);	
			if( bucket_depth == header->catalog_depth )
			{
				if( SEA_CACHED_OK != hash_table_catalog_multiplier( header, mmap_catalog, catalog_index ) )
				{
					DEBUG( "hash_table_catalog_multiplier error\n" );	
					return SEA_CACHED_ERROR;
				}

				uint32_t i = 0;
				for( i = (1<<bucket_depth); i < (1<<(bucket_depth+1)); ++i )
				{
					if( catalog_mirror == i )
						continue;
					struct CATALOG_INFO_T *cl = (struct CATALOG_INFO_T *)mmap_catalog->base + i;
					*cl = *( (struct CATALOG_INFO_T *)mmap_catalog->base + i - (1<<bucket_depth) );	
				}

				header->catalog_depth += 1;
			}

			//adjust buckets
			cl = (struct CATALOG_INFO_T *)mmap_catalog->base+catalog_index;
			memset( cl, 0, sizeof(struct CATALOG_INFO_T) );
			cl = (struct CATALOG_INFO_T *)mmap_catalog->base + catalog_mirror;
			memset( cl, 0, sizeof(struct CATALOG_INFO_T) );

			int32_t entry_next = entry_first; 
			while( -1 != entry_next )
			{	
				struct ENTRY_INFO_T *iter = (struct ENTRY_INFO_T *)mmap_entry->base + entry_next;
				int32_t next_tmp = iter->entry_next;
				if( ((iter->hash_code)&(1<<bucket_depth)) == catalog_mirror )
				{
					//insert into mirror catalog
					cl = (struct CATALOG_INFO_T *)mmap_catalog->base + catalog_mirror;
				} else {
					//insert into current catalog
					cl = (struct CATALOG_INFO_T *)mmap_catalog->base + catalog_index;
				}
				hash_table_adjust_bucket( mmap_entry, cl, iter, entry_next );			
				entry_next = next_tmp;
			}

			cl = (struct CATALOG_INFO_T *)mmap_catalog->base+catalog_index;
			cl->bucket_depth = (bucket_depth+1);
			cl = (struct CATALOG_INFO_T *)mmap_catalog->base + catalog_mirror;
			cl->bucket_depth = (bucket_depth+1);

			dump_catalog_info( (const struct CATALOG_INFO_T *)mmap_catalog->base+catalog_index );
			dump_catalog_info( (const struct CATALOG_INFO_T *)mmap_catalog->base+catalog_mirror );
			
			header->catalog.counter.count <<= 1;
		} else {
			if( NULL != pre )
				dump_entry_info( pre );
			break;
		}
	}

	//search and insert if unique
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
	new_entry->key_size = key->size;
	new_entry->value_size = value->size;
	new_entry->data_offset = header->data.counter.cursor;

	//insert entry to bucket chain
	if( NULL == pre )
	{
		new_entry->entry_next = (0 == entry_count)?-1:(cl->entry_first);
		cl->entry_first = entry_index;
	} else {
		struct ENTRY_INFO_T *tmp = (struct ENTRY_INFO_T *)pre;
		new_entry->entry_next = tmp->entry_next;
		tmp->entry_next = entry_index;	
	}	

	header->entry.counter.count += 1;
	if( header->entry_index < header->entry_total )
		header->entry_index += 1;

	header->data.counter.cursor += (key->size+value->size);
	
	cl->entry_count += 1;
	if( 0 != hash_collison_count )
		cl->hash_collision += 1;
	
	DEBUG( "hash_table_set OK\n" );
	
	return SEA_CACHED_OK;
}


static int32_t 
hash_table_get( const struct HASH_TABLE_T *hash_table, const struct VAR_BUF_T *key, struct VAR_BUF_T *value,
				uint32_t (*hash_callback)(const struct VAR_BUF_T *key), 
				int32_t (*uncompression_callback)( const struct VAR_BUF_T *key, struct VAR_BUF_T *res) )
{
	assert( NULL != hash_table && NULL != hash_table->header );
	assert( NULL != key && NULL != key->buf );
	assert( NULL != value );
	
	DEBUG( "hash_table_get:\n" );

	const struct HEADER_INFO_T *header = hash_table->header;

	dump_header_info( header );

	//hash_counting
	uint32_t hash_code = (NULL == hash_callback)?hash_calculating( key ):hash_callback( key );
	DEBUG( "hash_code:%u\n", hash_code );
	
	uint32_t catalog_depth = header->catalog_depth;
	uint32_t catalog_index = (0==catalog_depth)?0:hash_code&(1<<(catalog_depth-1));		
	DEBUG( "catalog_depth:%d catalog_index:%d\n", catalog_depth, catalog_index );

	const struct MMAP_INFO_T *mmap_catalog = &hash_table->catalog;
	const struct CATALOG_INFO_T *cl = (const struct CATALOG_INFO_T *)mmap_catalog->base+catalog_index;

	dump_catalog_info( cl );

	const struct ENTRY_INFO_T *entry = NULL;
	if( 0 != cl->entry_count )
	{
		int32_t ret = hash_table_search( hash_table, key, hash_code, cl->entry_first, &entry, NULL );
		if( SEA_CACHED_KEY_NON_EXIST == ret )
		{
			DEBUG( "nonexistence key\n" );
			return SEA_CACHED_KEY_NON_EXIST;	
		}
	} else {
		DEBUG( "nonexistence key\n" );
		return SEA_CACHED_KEY_NON_EXIST;	
	}

	dump_entry_info( entry );

	if( entry->value_size > value->length )
	{
		//reallocate
		DEBUG( "reallocate, size:%d\n", entry->value_size );
		if( NULL != value->buf )
			free( (void*)value->buf );

		value->buf = (void*)malloc( entry->value_size );
		if( NULL == value->buf )
		{
			DEBUG( "malloc error\n" );
			return SEA_CACHED_ERROR;
		}
		value->length = value->size;
	}

	const struct MMAP_INFO_T *mmap_data = &hash_table->data;
	memcpy( (void*)value->buf, (void*)( (char*)mmap_data->base+entry->data_offset+entry->key_size), entry->value_size );
	
	value->size = entry->value_size;
	DEBUG( "value size:%d\n", value->size );

	//uncompression
	//to do
	if( 0 )
	{
		if( NULL != uncompression_callback )
			uncompression_callback( NULL, NULL );
		else
			hash_table_data_uncompression( NULL, NULL );
	}

	DEBUG( "hash_table_get OK\n" );

	return SEA_CACHED_OK;
}


/*
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


static int32_t 
sea_cached_hash_table_close( const struct SEA_CACHED_T *cached, struct HASH_TABLE_T *hash_table )
{
	assert( NULL != cached && NULL != cached->file && NULL != cached->mmap );
	assert( NULL != hash_table && NULL != hash_table->header );

	DEBUG( "sea_cached_hash_table_close:\n" );

	const struct HEADER_INFO_T *header = hash_table->header;
	const struct FILE_INFO_T *file[] = { &header->catalog, &header->entry, &header->data };
	struct MMAP_INFO_T *mmap[] = { &hash_table->catalog, &hash_table->entry, &hash_table->data };

	dump_header_info( header );

	uint32_t i = 0;
	for( i = 0; i < sizeof(mmap)/sizeof(struct MMAP_INFO_T *); ++i )
	{
		if( NULL != (*mmap+i)->base )
		{	
			if ( -1 == msync( (*mmap+i)->base, (*file+i)->length, MS_SYNC ) )
			{
				DEBUG( "msync error:%s\n", strerror(errno) );		
				return SEA_CACHED_ERROR;
			}

			if( -1 == munmap( (*mmap+i)->base, (*file+i)->length ) )
			{
				DEBUG( "munmap error:%s\n", strerror(errno) );		
				return SEA_CACHED_ERROR;
			}

			close( (*mmap+i)->fd );
			(*mmap+i)->base = NULL;
			(*mmap+i)->fd = -1;
		}
	}
	
	if( NULL != cached->mmap->base )
	{	
		if ( -1 == msync( cached->mmap->base, cached->file->length, MS_SYNC ) )
		{
			DEBUG( "msync error:%s\n", strerror(errno) );		
			return SEA_CACHED_ERROR;
		}
	}
	
	//indicate this hashtable has been closed, shouldn't access again
	hash_table->flag = SEA_CACHED_HASHTABLE_CLOSE;

	return SEA_CACHED_OK;
}



static int32_t 
sea_cached_close( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached && NULL != cached->file && NULL != cached->mmap );

	DEBUG( "sea_cached_close:\n" );
	
	struct HASH_TABLE_T *hash_table = cached->hash_table;
	while( NULL != hash_table )
	{
		if( SEA_CACHED_OK != sea_cached_hash_table_close( cached, hash_table ) )
		{
			DEBUG( "sea_cached_hash_table_close error\n" );
			return SEA_CACHED_ERROR;	
		}

		struct HASH_TABLE_T *next = hash_table->next;	
		free( (void*)hash_table );
		hash_table = next;
	}

	if( NULL != cached->mmap->base )
	{	
		if ( -1 == msync( cached->mmap->base, cached->file->length, MS_SYNC ) )
		{
			DEBUG( "msync error:%s\n", strerror(errno) );		
			return SEA_CACHED_ERROR;
		}
		
		if( -1 == munmap( cached->mmap->base, cached->file->length ) )
		{
			DEBUG( "munmap error:%s\n", strerror(errno) );		
			return SEA_CACHED_ERROR;
		}

		close( cached->mmap->fd );
		cached->mmap->base = NULL;
		cached->mmap->fd = -1;
	}

	return SEA_CACHED_OK;
}


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


int main( int argc, char **argv )
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

	//sea_cached_hash_table_initial
	const char *table_name = "./test/test";
	struct HASH_TABLE_T *hash_table = sea_cached_hash_table_seach( cached.hash_table, table_name, strlen(table_name) );	
	uint32_t bucket_size = SEA_CACHED_BUCKET_SIZE;
	if ( NULL == hash_table && NULL == (hash_table = sea_cached_hash_table_create( &cached ,table_name, bucket_size ) ) )
	{
		DEBUG( "sea_cached_hash_table_create error\n" );
	}

	if( SEA_CACHED_OK != sea_cached_hash_table_initial( hash_table ) )
	{
		DEBUG( "sea_cached_hash_table_initial error\n" );
	}

	struct VAR_BUF_T key;
	struct VAR_BUF_T value;
	
	DEBUG( "key: %s value:%s\n", argv[2], argv[1] );
	const char *buf1 = argv[2];
	key.buf = (void*)buf1;
	key.size = strlen(buf1);
	
	const char *buf2 = argv[1];
	value.buf = (void*)buf2;
	value.size = strlen(buf2);

	struct timeval start;
	gettimeofday( &start, NULL );
	hash_table_set( hash_table, &key, &value, NULL, NULL );
	struct timeval stop;
	gettimeofday( &stop, NULL );

	DEBUG( "%d us\n", ((int32_t)stop.tv_sec-(int32_t)start.tv_sec)*1000000+(int32_t)stop.tv_usec-(int32_t)start.tv_usec );

	struct VAR_BUF_T res;
	memset( &res, 0, sizeof(res) );
	
	gettimeofday( &start, NULL );
	hash_table_get( hash_table, &key, &res, NULL, NULL );
	gettimeofday( &stop, NULL );
	
	DEBUG( "%d us\n", ((int32_t)stop.tv_sec-(int32_t)start.tv_sec)*1000000+(int32_t)stop.tv_usec-(int32_t)start.tv_usec );
	DEBUG( "value size:%d value:%s\n", res.size, (char*)res.buf );
	if( NULL != res.buf )
		free( (void*)res.buf );

	sea_cached_close( &cached );

	return 0;
}

