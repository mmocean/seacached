/*************************************************************************
	> File Name: example.c
	> Author: lihy
	> Mail: lihaiyangcsu@gmail.com 
	> Created Time: 2015-08-11
 ************************************************************************/

#include <stdio.h>		//snprintf printf
#include <stdlib.h>		//malloc free
#include <string.h>		//strerror memset strncpy
#include <sys/time.h>	//gettimeofday
#include <unistd.h>		//lseek close write sysconf read getpid
#include "def.h"

static int32_t example( int argc, char **argv )
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

	return SEA_CACHED_OK;
}


int main( int argc, char **argv )
{
	(void)example( argc, argv );

	return 0;
}


