/*************************************************************************
	> File Name: seacached.c
	> Author: lihy
	> Mail: lihaiyangcsu@gmail.com 
	> Created Time: 2015-07-28
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <fcntl.h>//O_RDWR O_CREAT S_IRWXU open
#include <unistd.h>//lseek close write
#include <sys/stat.h>
#include "def.h"

static int32_t 
sea_cached_content_write( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value );

static int32_t 
sea_cached_file_open( struct SEA_CACHED_T *cached );

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
		int32_t i = 2;
		for( i = 2; i < (bucket_number>>1)+1; ++i )
		{
			if( 0 == bucket_number%i )	
				break;
		}
		if( i == (bucket_number>>1)+1 )
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
		"%s:%d\n %s:%d\n %s:%d\n %s:%d\n %s:%d\n %s:%d\n %s:%d\n %s:%d\n %s:%d\n %s:%d\n %s:%u\n %s:%u\n %s:%u\n %s:%u\n", 
		"version", header->version,
		"flag", header->flag,
		"filter_size", header->filter_size,
		"page_size", header->page_size,
		"bucket_number", header->bucket_number,	
		"bucket_size", header->bucket_size,	
		"entity_number", header->entity_number,	
		"entity_count", header->entity_count,	
		"content_size", header->content_size,
		"file_length", header->file_length,
		"filter_offset", header->filter_offset,
		"bucket_offset", header->bucket_offset,
		"entity_offset", header->entity_offset,
		"content_offset", header->content_offset );
	
	printf( "dump_header_info:\n %s\n", buf );
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
	if( header->bucket_number < 0 )
	{
		printf( "bucket_number_counting error\n" );
		return -1;
	}

	header->page_size = getpagesize();

	offset_counting( header );
	
	return 0;
}


static int32_t 
sea_cached_initial( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );

	if ( sea_cached_file_open( cached ) != SEA_CACHED_OK )
	{
		printf( "sea_cached_file_open error\n" );
		return SEA_CACHED_ERROR;
	}
	
	return SEA_CACHED_OK;
}


/*
 * exists:SEA_CACHED_OK is returned, copy content to value 
 * otherwise:SEA_CACHED_ERROR is returned, copy previous entity to 
 * */
/*
static int32_t
sea_cached_search( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, uint32_t hash_code, struct ENTITY *entity, struct VAR_BUF_T *value, int32_t force_cover = 0 )
{
	assert( NULL != cached && NULL != cached->header );

	struct ENTITY *entity_base = (struct ENTITY*)( (char*)cached->entity_base );
	struct BUCKET *bucket_base = (struct BUCKET*)( (char*)cached->bucket_base );

	int32_t bucket_index = hash_code%cached->header->bucket_number;

	//search
	int32_t entity_number = (bucket_base+bucket_index)->entity_number;
	if( 0 == entity_number )
	{
		return SEA_CACHED_KEY_NON_EXIST;	
	}

	int32_t next = (bucket_base+bucket_index)->first_entity;
	int32_t pre = next;
	for( int i = 0; i<entity_number; ++i )
	{
		const struct ENTITY *tmp = entity_base+next;

		if( hash_code < tmp->hash_code )
			break;

		if( hash_code == tmp->hash_code && ((tmp->kv_size>>22)&0x400) == key->size )
		{
			if( 0 == memcmp( (void*)((char*)content+tmp->content_index), (void*)key->buf, key->size ) )
			{
				if( NULL != value )
				{
					//to do	
				}
				return SEA_CACHED_KEY_EXIST;
			}
		}	

		pre = next;
		next = entity->entity_next;
	}

	if( pre != next && NULL != entity )
	{
		memcpy( (void*)entity, (void*)(entity_base+pre), sizeof(struct ENTITY) );
	}

	return SEA_CACHED_KEY_NON_EXIST;	
}
*/

static int32_t 
sea_cached_set( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value, int32_t force_cover )
{
	assert( NULL != cached && NULL != cached->header );
	assert( NULL != key && NULL != key->buf );
	assert( NULL != value && NULL != value->buf );
	
	struct HEADER *header = cached->header;

	//be full
	printf( "header->entity_count:%d\n", header->entity_count );
	if( header->entity_count > header->entity_number )
		return -1;

	//hash_counting
	uint32_t hash_code = hash_counting_wrapper( key );
	printf( "hash_code:%u\n", hash_code );

	struct ENTITY *entity_base = (struct ENTITY*)( cached->entity_base );
	struct BUCKET *bucket_base = (struct BUCKET*)( cached->bucket_base );

	int32_t bucket_index = hash_code%header->bucket_number;
	printf( "bucket_index:%d\n", bucket_index );

	//search
	int32_t entity_number = (bucket_base+bucket_index)->entity_number;
	struct ENTITY *pre = NULL;
	if( 0 != entity_number )
	{
		int32_t next = (bucket_base+bucket_index)->first_entity;
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
					printf( "key already exists\n" );
					return SEA_CACHED_KEY_EXIST;
				}
			}	

			pre = tmp;
			next = tmp->entity_next;
		}

	}

	int32_t index = 0;
	for( index = 0; index<header->entity_number; ++index )
	{
		//un-used
		struct ENTITY entity;
		memset( &entity, 0, sizeof(struct ENTITY) );
		if( 0 == memcmp( (void*)(entity_base+index), &entity, sizeof(struct ENTITY) ) )
			break;	
	}
	printf( "available entity index:%d\n", index );
	
	sea_cached_content_write( cached, key, value );

	struct ENTITY entity;
	entity.hash_code = hash_code;
	entity.kv_size = 0;
	entity.kv_size |= (key->size<<22);
	entity.kv_size |= (value->size);
	entity.content_index = cached->file_position;
	
	if( 0 == entity_number || NULL == pre )
	{
		(bucket_base+bucket_index)->first_entity = index;
		entity.entity_next = -1;
	} else {
		entity.entity_next = pre->entity_next;
		pre->entity_next = index;	
	}

	++header->entity_count;	
	header->content_size += (key->size+value->size);
	
	memcpy( (void*)(entity_base+index), (void*)&entity, sizeof(struct ENTITY) );
	++(bucket_base+bucket_index)->entity_number;
	cached->file_position += (key->size+value->size);


	return SEA_CACHED_OK;
}


static int32_t 
sea_cached_get( const struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, struct VAR_BUF_T *value )
{
	assert( NULL != cached );

	return SEA_CACHED_OK;
}

static int32_t 
sea_cached_delete( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key )
{
	assert( NULL != cached );

	return SEA_CACHED_OK;
}


static int32_t 
sea_cached_file_open( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );

	const char *filename = "seacached.index";
	int32_t fd = open( filename, O_RDWR|O_CREAT, S_IRWXU );
	if( -1 == fd )
	{
		printf( "open error\n" );
		return SEA_CACHED_ERROR;
	}
	
	struct stat st;
	if( -1 == fstat( fd, &st ) )
	{
		printf( "fstat error\n" );
		return SEA_CACHED_ERROR;	
	}

	printf( "filesize:%d\n", (int)st.st_size );

	struct HEADER header;
	memset( &header, 0, sizeof(struct HEADER) );
	
	if( st.st_size > sizeof(struct HEADER) )
	{
		ssize_t size = read( fd, (void*)&header, sizeof(struct HEADER) );
		printf( "read size:%d\n", size );
		if( st.st_size != header.file_length )
		{
			printf( "file length error\n" );
			return SEA_CACHED_ERROR;
		}
	} else if( 0 == st.st_size ) {

		if( header_initial( &header ) < 0 )
		{
			printf( "header_initial error\n" );
			return -1;
		}

		if( -1 == lseek( fd, header.file_length-1, SEEK_SET ) )
		{
			printf( "lseek error\n" );
			close( fd );
			return SEA_CACHED_ERROR;
		}
		char nil = 0;
		ssize_t size = write( fd, (void*)&nil, 1 );
		printf( "write size:%d\n", size );

	} else {
		printf( "file format error\n" );
		return SEA_CACHED_ERROR;	
	}

	dump_header_info( &header );	

	void *mmap_base = (void*)mmap( NULL, header.file_length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0 );
	if( (void*)-1 == mmap_base )
	{
		printf( "mmap error\n" );
		close( fd );
		return SEA_CACHED_ERROR;
	}

	cached->mmap_base = mmap_base;
	cached->header = &header;
	cached->file_position = header.content_offset + header.content_size;

	base_counting( cached );

	if( 0 == st.st_size ) 
	{
		memcpy( (void*)mmap_base, (void*)&header, sizeof(struct HEADER) );
		memset( (char*)cached->filter_base, 0, cached->header->bucket_offset - cached->header->filter_offset );
		memset( (char*)cached->bucket_base, 0, cached->header->entity_offset - cached->header->bucket_offset );
		memset( (char*)cached->entity_base, 0, cached->header->content_offset - cached->header->entity_offset );
	}

	cached->fd = fd;
	cached->header = (struct HEADER*)mmap_base;

	return SEA_CACHED_OK;
}




static int32_t 
sea_cached_file_sysnc( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );
	return SEA_CACHED_OK;
}

static int32_t 
sea_cached_file_close( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );
	
	munmap( cached->mmap_base, cached->header->file_length );
	close( cached->fd );

	return SEA_CACHED_OK;
}



static int32_t 
sea_cached_content_extension( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );

	printf( "sea_cached_content_extension error\n" );	

	return SEA_CACHED_OK;
}


static int32_t 
sea_cached_content_write( struct SEA_CACHED_T *cached, const struct VAR_BUF_T *key, const struct VAR_BUF_T *value )
{
	assert( NULL != cached );
	
	struct HEADER *header = cached->header;
	
	printf( "header->file_length:%d cached->file_position:%d\n", header->file_length, cached->file_position );
	
	if( (header->file_length-cached->file_position) < (key->size+value->size) )
	{
		//to do
		if( sea_cached_content_extension( cached ) != SEA_CACHED_OK )	
		{
			printf( "sea_cached_content_extension error\n" );	
			return SEA_CACHED_ERROR;
		}
	}

	memcpy( (void*)((char*)cached->mmap_base+cached->file_position), (void*)key->buf, key->size );
	memcpy( (void*)((char*)cached->mmap_base+cached->file_position+key->size), (void*)value->buf, value->size );

	return SEA_CACHED_OK;
}




static int32_t 
sea_cached_capacity_extension( struct SEA_CACHED_T *cached )
{
	assert( NULL != cached );
	return SEA_CACHED_OK;
}



int main()
{	
	struct SEA_CACHED_T cached;
	memset( &cached, 0, sizeof(struct SEA_CACHED_T) );

	if( sea_cached_initial( &cached ) != SEA_CACHED_OK )
	{
		printf( "sea_cached_initial error\n" );
	}

	struct VAR_BUF_T key;
	struct VAR_BUF_T value;
	
	char *s1 = "1234568";
	char *s2 = "xxxxx";
	key.size = strlen(s1);
	key.buf = s1;
	value.size = strlen(s2);
	value.buf = s2;

	sea_cached_set( &cached, &key, &value, 0 );

	sea_cached_file_close( &cached );

	return 0;
}

