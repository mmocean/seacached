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
sea_cached_content_write( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value );

static int32_t 
sea_cached_content_extension( struct SEA_CACHED_T *cached, int32_t extend_size );

static int32_t 
sea_cached_file_close( struct SEA_CACHED_T *cached );

static int32_t 
sea_cached_set( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value, int32_t force_cover );


static int32_t 
data_compression_wrapper( const struct VAR_BUF_T *key, struct VAR_BUF_T *res )
{
	assert( NULL != key && NULL != key->buf );
	assert( NULL != res && NULL != res->buf );

	return SEA_CACHED_OK;
}

static uint32_t 
hash_counting_wrapper( const struct VAR_BUF_T *key )
{
	assert( NULL != key && NULL != key->buf );

	//BKDRHash
	unsigned int seed = 131; //magic number
	const char *str = (const char *)key->buf;
	
	uint32_t hash_code = 0;
	while( '\0' != *str )
	{
		hash_code = hash_code*seed + (*str++);
	}

	return hash_code&0x7fffffff;
}


static int32_t 
bucket_number_counting( int32_t entity_number, int32_t bucket_average_size )
{
	assert( entity_number > 0 && bucket_average_size > 0 );
	
	//to count the smallest prime greater than entity_size/bucket_average_size
	int32_t bucket_number = entity_number/bucket_average_size;
	while( ++bucket_number > 0 )
	{
		int32_t n = (bucket_number>>1)+1;
		int32_t i = 2;
		for( i = 2; i < n; ++i )
		{
			if( 0 == bucket_number%i )	
				break;
		}
		if( i == n )
			return bucket_number;
	}

	return SEA_CACHED_ERROR;
}

static void
dump_header_info( struct HEADER *header )
{
	assert( NULL != header );
	
	char buf[512];
	memset( buf, 0, sizeof(buf) );

	snprintf( buf, sizeof(buf), 
		"%s: %d\n %s: %d\n %s: %d\n %s: %d\n "
		"%s: %d\n %s: %d\n %s: %d\n %s: %d\n " 
		"%s: %d\n %s: %d\n %s: %d\n "
		"%s: %u\n %s: %u\n %s: %u\n %s: %u\n", 
		"version", header->version,
		"flag", header->flag,
		"filter_size", header->filter_size,
		"page_size", header->page_size,
		"bucket_number", header->bucket_number,	
		"bucket_size", header->bucket_size,	
		"entity_number", header->entity_number,	
		"entity_count", header->entity_count,	
		"entity_incre", header->entity_incre,
		"content_size", header->content_size,
		"file_length", header->file_length,
		"filter_offset", header->filter_offset,
		"bucket_offset", header->bucket_offset,
		"entity_offset", header->entity_offset,
		"content_offset", header->content_offset );
	
	DEBUG( "dump_header_info:\n %s\n", buf );
}

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


static int32_t 
sea_cached_initial( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );

	if ( sea_cached_content_extension( cached, 0 ) != SEA_CACHED_OK )
	{
		DEBUG( "sea_cached_content_extension error\n" );
		close( cached->fd );
		return SEA_CACHED_ERROR;
	}
	
	return SEA_CACHED_OK;
}


/*
 * exists:SEA_CACHED_OK is returned, copy content to value 
 * otherwise:SEA_CACHED_ERROR is returned, copy previous entity to 
 * */

static int32_t
sea_cached_search( const struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, uint32_t hash_code, int32_t entity_number, int32_t bucket_index, int32_t force_cover, int32_t call_flag, struct ENTITY **entity )
{
	assert( NULL != cached );

	DEBUG( "sea_cached_search:\n" );	
	
	//search
	struct BUCKET *bucket_base = (struct BUCKET*)( cached->bucket_base );
	int32_t next = (bucket_base+bucket_index)->first_entity;

	struct ENTITY *entity_base = (struct ENTITY*)( cached->entity_base );
	struct ENTITY *pre = NULL;
	int i = 0;
	for( i = 0; i<entity_number; ++i )
	{
		struct ENTITY *tmp = entity_base+next;

		if( hash_code < tmp->hash_code )
			break;

		if( hash_code == tmp->hash_code && ((tmp->kv_size>>22)&0x3ff) == key->size )
		{
			if( 0 == memcmp( (void*)((char*)cached->mmap_base+tmp->content_index), (void*)key->buf, key->size ) )
			{
				DEBUG( "key already exists\n" );
				if( SEA_CACHED_FORCE_COVER == force_cover || SEA_CACHED_GET_CALL == call_flag )
				{
					*entity = tmp;						
				} else {
					*entity = pre;	
				}
				return SEA_CACHED_KEY_EXIST;
			}
		}	
		pre = tmp;
		next = tmp->entity_next;
	}

	*entity = pre;							
	DEBUG( "nonexistence key\n" );

	return SEA_CACHED_KEY_NON_EXIST;	
}


static int32_t 
available_entity_search( const struct ENTITY *entity_base, int32_t entity_number )
{
	assert( NULL != entity_base );

	int32_t index = 0;
	for( index = 0; index<entity_number; ++index )
	{
		//un-used
		struct ENTITY *entity = (struct ENTITY *)(entity_base+index);
		if( 0 == entity->hash_code && -1 == entity->entity_next )
		{
			DEBUG( "available entity index:%d\n", index );
			return index;	
		}
	}

	return SEA_CACHED_ERROR;
}

static int32_t 
sea_cached_set( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value, int32_t force_cover )
{
	assert( NULL != cached && NULL != cached->header );
	assert( NULL != key && NULL != key->buf );
	assert( NULL != value && NULL != value->buf );
	
	DEBUG( "sea_cached_set:\n" );
	
	struct HEADER *header = cached->header;
	
	//be full
	DEBUG( "header->entity_count:%d\n", header->entity_count );
	if( header->entity_count > header->entity_number )
	{
		return SEA_CACHED_ERROR;
	}

	//hash_counting
	uint32_t hash_code = hash_counting_wrapper( key );
	DEBUG( "hash_code:%u\n", hash_code );

	int32_t bucket_index = hash_code%header->bucket_number;
	DEBUG( "bucket_index:%d\n", bucket_index );

	struct BUCKET *bucket_base = (struct BUCKET*)( cached->bucket_base );
	int32_t entity_number = (bucket_base+bucket_index)->entity_number;
	DEBUG( "entity_number of bucket:%d\n", entity_number );
	
	struct ENTITY *pre = NULL;
	if( 0 != entity_number )
	{
		int32_t call_flag = SEA_CACHED_SET_CALL; 
		int32_t ret = sea_cached_search( cached, key, hash_code, entity_number, bucket_index, force_cover, call_flag, &pre );
		if( SEA_CACHED_KEY_EXIST == ret && SEA_CACHED_FORCE_COVER != force_cover )
		{
			DEBUG( "duplicate key\n" );	
			return SEA_CACHED_ERROR;	
		}
	}

	struct ENTITY *entity_base = (struct ENTITY*)( cached->entity_base );
	int32_t index = -1;
	if( header->entity_incre >= header->entity_number )
	{
		index = available_entity_search( entity_base, header->entity_number );
		if( SEA_CACHED_ERROR == index ) 
		{
			DEBUG( "available_entity_search error\n" );	
			return SEA_CACHED_ERROR;
		}
	} else {
		//direct allocation
		index = header->entity_incre;
	}

	//write key-value to file
	if( sea_cached_content_write( cached, key, value ) != SEA_CACHED_OK )
	{
		DEBUG( "sea_cached_content_write error\n" );	
		return SEA_CACHED_ERROR;
	}

	struct ENTITY entity;
	entity.hash_code = hash_code;
	entity.kv_size = 0;
	entity.kv_size |= (key->size<<22);
	entity.kv_size |= (value->size);
	entity.content_index = cached->content_cursor;

	//insert entity to chain
	if( 0 == entity_number || NULL == pre )
	{
		(bucket_base+bucket_index)->first_entity = index;
		entity.entity_next = -1;
	} else {
		entity.entity_next = pre->entity_next;
		pre->entity_next = index;	
	}	
	memcpy( (void*)(entity_base+index), (void*)&entity, sizeof(struct ENTITY) );
	
	(bucket_base+bucket_index)->entity_number += 1;
	
	header->entity_count += 1;
	header->content_size += (key->size+value->size);
	if( header->entity_incre < header->entity_number )
		header->entity_incre += 1;

	cached->content_cursor += (key->size+value->size);

	DEBUG( "sea_cached_set OK\n" );
	
	return SEA_CACHED_OK;
}


static int32_t 
sea_cached_get( const struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, struct VAR_BUF_T *value )
{
	assert( NULL != cached && NULL != cached->header );
	assert( NULL != key && NULL != key->buf );
	assert( NULL != value );
	
	DEBUG( "sea_cached_get:\n" );
	
	struct HEADER *header = cached->header;
	
	//hash_counting
	uint32_t hash_code = hash_counting_wrapper( key );
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
		int32_t ret = sea_cached_search( cached, key, hash_code, entity_number, bucket_index, 0, call_flag, &entity );
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

	DEBUG( "sea_cached_get OK\n" );

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
	uint32_t hash_code = hash_counting_wrapper( key );
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
		int32_t ret = sea_cached_search( cached, key, hash_code, entity_number, bucket_index, 0, call_flag, &entity );
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


static int32_t 
sea_cached_content_extension( struct SEA_CACHED_T *cached, int32_t extend_size )
{
	assert( NULL != cached );
	assert( NULL != cached->file_name );

	DEBUG( "sea_cached_content_extension:\n" );	

	if( 0 != extend_size && SEA_CACHED_OK != sea_cached_file_close( cached ) )
	{
		DEBUG( "sea_cached_file_close error\n" );		
		return SEA_CACHED_ERROR;
	}

	//S_IRWXU: 00700 user (file owner) has  read,  write  and  execute permission
	int32_t fd = open( cached->file_name, O_RDWR|O_CREAT, S_IRWXU );
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

	struct HEADER header;
	memset( &header, 0, sizeof(struct HEADER) );
	int new_file_flag = 0;

	if( st.st_size > sizeof(struct HEADER) )
	{
		ssize_t size = read( fd, (void*)&header, sizeof(struct HEADER) );
		DEBUG( "read size:%d\n", size );
		if( -1 == size )
		{
			DEBUG( "read error:%s\n", strerror(errno) );
			return SEA_CACHED_ERROR;	
		}

		if( st.st_size != header.file_length )
		{
			DEBUG( "file length does not match\n" );
			return SEA_CACHED_ERROR;
		}
	} else if( 0 == st.st_size ) {
		new_file_flag = 1;

		if( header_initial( &header ) != SEA_CACHED_OK )
		{
			DEBUG( "header_initial error\n" );
			return -1;
		}
	} else {
		DEBUG( "file format error\n" );
		return SEA_CACHED_ERROR;	
	}
	
	header.file_length += extend_size;
	if( -1 == lseek( fd, header.file_length-1, SEEK_SET ) )
	{
		DEBUG( "lseek error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}

	char nil = 0;
	ssize_t size = write( fd, (void*)&nil, sizeof(char) );
	DEBUG( "write size:%d\n", size );
	if( -1 == size )
	{
		DEBUG( "write error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;	
	}

	dump_header_info( &header );	
	
	off_t off = 0;
	void *mmap_base = (void*)mmap( NULL, header.file_length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, off );
	if( MAP_FAILED == mmap_base )
	{
		DEBUG( "mmap error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}

	if( -1 == madvise( mmap_base, header.file_length, MADV_NORMAL ) )
	{
		DEBUG( "madvise error:%s\n", strerror(errno) );
		return SEA_CACHED_ERROR;
	}
	cached->mmap_base = mmap_base;
	cached->header = &header;
	cached->content_cursor = header.content_offset + header.content_size;

	base_counting( cached );

	if( new_file_flag ) 
	{
		memcpy( (void*)mmap_base, (void*)&header, sizeof(struct HEADER) );
		memset( (char*)cached->filter_base, 0, cached->header->bucket_offset - cached->header->filter_offset );
		memset( (char*)cached->bucket_base, 0, cached->header->entity_offset - cached->header->bucket_offset );
		memset( (char*)cached->entity_base, 0, cached->header->content_offset - cached->header->entity_offset );
	}

	cached->fd = fd;
	cached->header = (struct HEADER*)mmap_base;
	cached->header->file_length += extend_size;

	return SEA_CACHED_OK;
}


static int32_t 
sea_cached_file_sysnc( struct SEA_CACHED_T *cached, int32_t flag )
{
	assert( NULL != cached );

	if( NULL != cached && NULL != cached->mmap_base && NULL != cached->header )
	{
		if ( -1 == msync( cached->mmap_base, cached->header->file_length, flag ) )
		{
			DEBUG( "msync error:%s\n", strerror(errno) );		
			return SEA_CACHED_ERROR;
		}
	}

	return SEA_CACHED_OK;
}


static int32_t 
sea_cached_file_munmap( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );

	if( NULL != cached && NULL != cached->mmap_base && NULL != cached->header )
	{
		if( -1 == munmap( cached->mmap_base, cached->header->file_length ) )
		{
			DEBUG( "munmap error:%s\n", strerror(errno) );		
			return SEA_CACHED_ERROR;
		}
	}

	return SEA_CACHED_OK;
}


static int32_t 
sea_cached_file_close( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );

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


static int32_t 
sea_cached_content_write( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value )
{
	assert( NULL != cached );
	
	struct HEADER *header = cached->header;
	
	DEBUG( "header->file_length:%d cached->content_cursor:%d\n", header->file_length, cached->content_cursor );
	DEBUG( "key->size:%d value->size:%d\n", key->size, value->size );
	
	if( (header->file_length-cached->content_cursor) < (key->size+value->size) )
	{
		//to do
		int32_t size = CONTENT_EXTENSION_SIZE;
		if( sea_cached_content_extension( cached, size ) != SEA_CACHED_OK )	
		{
			DEBUG( "sea_cached_content_extension error\n" );	
			return SEA_CACHED_ERROR;
		}
	}

	//to do
	if( 0 )
	{
		data_compression_wrapper( NULL, NULL );
	}

	memcpy( (void*)((char*)cached->mmap_base+cached->content_cursor), (void*)key->buf, key->size );
	memcpy( (void*)((char*)cached->mmap_base+cached->content_cursor+key->size), (void*)value->buf, value->size );

	return SEA_CACHED_OK;
}


static int32_t 
sea_cached_capacity_extension( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );

	DEBUG( "sea_cached_capacity_extension:\n" );	

	return SEA_CACHED_OK;
}


int main()
{	
	struct SEA_CACHED_T cached;
	memset( &cached, 0, sizeof(struct SEA_CACHED_T) );
	cached.file_name = "seacached.index";

	if( sea_cached_initial( &cached ) != SEA_CACHED_OK )
	{
		DEBUG( "sea_cached_initial error\n" );
	}

	struct VAR_BUF_T key;
	struct VAR_BUF_T value;
	
	key.buf = "1234568";
	key.size = strlen(key.buf);
	
	value.buf = "ABCD";
	value.size = strlen(value.buf);

	struct timeval start;
	gettimeofday( &start, NULL );
	sea_cached_set( &cached, &key, &value, 0 );
	struct timeval stop;
	gettimeofday( &stop, NULL );

	DEBUG( "%d %d \n", (int32_t)start.tv_sec, (int32_t)start.tv_usec );
	DEBUG( "%d %d \n", (int32_t)stop.tv_sec, (int32_t)stop.tv_usec );

	struct VAR_BUF_T res;
	memset( &res, 0, sizeof(res) );
	
	gettimeofday( &start, NULL );
	sea_cached_get( &cached, &key, &res );
	gettimeofday( &stop, NULL );
	
	DEBUG( "%d %d \n", (int32_t)start.tv_sec, (int32_t)start.tv_usec );
	DEBUG( "%d %d \n", (int32_t)stop.tv_sec, (int32_t)stop.tv_usec );

	DEBUG( "value size:%d value:%s\n", res.size, (char*)res.buf );
	if( NULL != res.buf )
		free( (void*)res.buf );

	sea_cached_delete( &cached, &key );

	sea_cached_file_close( &cached );

	return 0;
}

