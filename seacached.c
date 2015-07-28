/*************************************************************************
	> File Name: seacached.c
	> Author: lihy
	> Mail: lihaiyangcsu@gmail.com 
	> Created Time: 2015-07-28
 ************************************************************************/

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include"def.h"


int32_t hash_counting( void *key, int32_t key_length )
{
	int32_t hash_code = 0;

	return hash_code&0x7fffffff;
}


int32_t bucket_number_counting( int32_t entity_number, int32_t bucket_average_size )
{
	//to count the smallest prime greater than entity_size/bucket_average_size
	if( entity_number <= 0 || bucket_average_size <= 0 )
		return -1;
	
	int32_t bucket_number = entity_number/bucket_average_size;
	while( ++bucket_number > 0 )
	{
		int32_t i = 2;
		for( ; i < (bucket_number>>1)+1; ++i )
		{
			if( 0 == bucket_number%i )	
				break;
		}
		if( i == (bucket_number>>1)+1 )
			return bucket_number;
	}

	return -1;
}

/*
int32_t fixed_space_counting( struct HEADER *header )
{
	if( NULL == header || header->bucket_number <= 0 || header->entity_number <= 0 )
		return -1;

	int32_t fixed_space = 0;

	fixed_space += sizeof( struct HEADER );
	fixed_space += sizeof( struct BUCKET ) * header->bucket_number;
	fixed_space += sizeof( struct ENTITY ) * header->entity_number;

	fixed_space += header->filter_size;
	
	return fixed_space;
}
*/

int32_t offset_base_counting( struct HEADER *header )
{
	if( NULL == header )
		return -1;
	
	header->filter_offset = sizeof( struct HEADER );
	header->bucket_offset = header->filter_offset + header->filter_size;
	header->entity_offset = header->bucket_offset + sizeof(struct BUCKET) * header->bucket_number;
	header->content_offset = header->entity_offset + sizeof(struct ENTITY) * header->entity_number;

	return 0;
}


int32_t header_initial( struct HEADER *header )
{
	if( NULL == header )
		return -1;
	
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

	if( offset_base_counting( header ) < 0 )
	{
		printf( "offset_base_counting error\n" );
		return -1;
	}
	
	return 0;
}


int32_t sea_cached_initial( struct SEACACHED *cached )
{
	if( NULL == cached )
		return -1;

	if( header_initial( cached->header ) < 0 )
	{
		printf( "header_initial error\n" );
		return -1;
	}

	const void* mmap_base = cached->mmap_base;
	const struct HEADER* header = cached->header;	

	cached->filter_base = (void*)((char*)mmap_base+header->filter_offset);
	cached->bucket_base = (void*)((char*)mmap_base+header->entity_offset);
	cached->entity_base = (void*)((char*)mmap_base+header->entity_offset);
	cached->content_base = (void*)((char*)mmap_base+header->content_offset);

	return 0;
}

struct ENTITY* search( const struct ENTITY *entity, const void *content, void *key, int32_t key_length, uint32_t hash_code )
{
	if( NULL == entity || NULL == content )
		return NULL;
	
	int32_t next = 0;
	int32_t pre = 0;
	while( -1 != next )
	{
		entity += next;
		if( hash_code == entity->hash_code && ((entity->kv_size>>22)&0x400) == key_length )
		{
			if( 0 == memcmp( (void*)((char*)content+entity->content_index), key, key_length ) )
				return (struct ENTITY*)entity;
		}	
		if( hash_code < entity->hash_code )
			break;

		pre = next;
		next = entity->entity_next;
	}

	return (struct ENTITY*)(entity+pre);
}


int32_t set( struct SEACACHED *cached, void *key, int32_t key_length, void *value, int32_t value_length )
{
	if( NULL == cached || NULL == key )
		return -1;
	
	struct HEADER *header = cached->header;
	if( NULL == header )
		return -1;

	//be full
	if( header->entity_count > header->entity_number )
		return -1;

	//hash_counting
	int32_t hash_code = hash_counting( key, key_length );
	if( hash_code < 0 )
		return -1;

	struct ENTITY *entity_base = (struct ENTITY*)( (char*)cached->entity_base );
	if( NULL == entity_base )
		return -1;

	struct BUCKET *bucket_base = (struct BUCKET*)( (char*)cached->bucket_base );
	if( NULL == bucket_base )
		return -1;
	int32_t bucket_index = hash_code%header->bucket_number;

	//search
	int32_t first_entity = (bucket_base+bucket_index)->first_entity;
	if( -1 != first_entity )
	{
		if( NULL == search( entity_base+first_entity, (void*)( (char*)cached->content_base ), key, key_length, hash_code ) )
			return -1;
	} 

	int32_t i = 0;
	for( ; i<header->entity_number; ++i )
	{
		//un-used
		if( entity_base->kv_size == 0 )
			break;	
	}
	
	struct ENTITY entity;
	entity.hash_code = hash_code;
	entity.kv_size |= (key_length<<22);
	entity.kv_size |= (value_length);




	return 0;
}



int main()
{
	struct HEADER header;
	memset( &header, 0, sizeof(struct HEADER) );
	
	struct SEACACHED cached;
	memset( &cached, 0, sizeof(struct SEACACHED) );
	cached.header = &header;

	if( sea_cached_initial( &cached ) < 0 )
	{
		printf( "sea_cached_initial error\n" );
		return -1;
	}
	
	return 0;
}

