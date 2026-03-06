/*
 * Copyright 2023 The Nodepp Project Authors. All Rights Reserved.
 *
 * Licensed under the MIT (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://github.com/NodeppOfficial/nodepp/blob/main/LICENSE
 */

/*────────────────────────────────────────────────────────────────────────────*/

#ifndef NODEPP_MONGODB
#define NODEPP_MONGODB

/*────────────────────────────────────────────────────────────────────────────*/

#include <nodepp/nodepp.h>
#include <nodepp/expected.h>
#include <mongoc/mongoc.h>
#include <nodepp/json.h>
#include <bson/bson.h>

/*────────────────────────────────────────────────────────────────────────────*/

namespace nodepp { void inline start_mongodb() {
    static atomic_t<bool> enabled = false; 
    if( enabled.get(true) )/**/{ return; }
    nodepp::process::onSIGEXIT.once([=](){
        mongoc_cleanup(); 
    }); mongoc_init(); 
}}

/*────────────────────────────────────────────────────────────────────────────*/

namespace nodepp { class mongodb_t {
protected:

    struct NODE {

        mongoc_uri_t    *uri = nullptr;
        mongoc_client_t *ctx = nullptr;
        map_t<string_t, mongoc_collection_t*> list;

        ~NODE() {
            auto x = list.raw().first(); while( x!=nullptr ){
            auto y = x->next; 
                mongoc_collection_destroy( x->data );
            x=y; }
            if( ctx ){ mongoc_client_destroy(ctx); }
            if( uri ){ mongoc_uri_destroy   (uri); }
        }
        
    };  ptr_t<NODE> obj;

    mongoc_collection_t* get_collection( string_t db, string_t table ) const noexcept {
        string_t id = regex::format( "${0}_${1}", db, table );
        if( obj->list.has(id) )/*-*/{ return obj->list[id]; }
        return obj->list[id] = mongoc_client_get_collection( obj->ctx, db.get(), table.get() );
    }

    bson_t* json_to_bson( object_t json_obj ) const {
        if( json_obj.empty() ){ return nullptr; }

        bson_error_t error;
        string_t raw = json::stringify( json_obj );
        bson_t*  doc = bson_new_from_json( (const uint8_t*) raw.get(), raw.size(), &error ); 

    return !doc ? nullptr : doc; }

    object_t bson_to_json( const bson_t* bson_obj ) const {
        if( !bson_obj ){ return nullptr; }

        size_t len;
        char*  str = bson_as_canonical_extended_json( bson_obj, &len );
        object_t out = json::parse( string_t( str, len ) );

    bson_free(str); return out; }

public:

    mongodb_t( string_t uri_str ) : obj( new NODE() ) { start_mongodb(); 

        bson_error_t error;
        obj->uri = mongoc_uri_new_with_error( uri_str.get(), &error );
        if( !obj->uri ) { throw except_t( "Failed to parse URI:", error.message ); }
        
        obj->ctx = mongoc_client_new_from_uri( obj->uri );
        if( !obj->ctx ) { throw except_t( "Failed to create MongoDB client" ); }

    }

    mongodb_t() : obj( new NODE() ) { start_mongodb(); }

    /*─······································································─*/

    expected_t<array_t<object_t>,except_t>
    find( string_t db, string_t table, object_t filter={}, object_t options={} ) const noexcept {
        
        auto collection  = get_collection( db, table );

        bson_t* b_filter = json_to_bson( filter  ); if( b_filter==nullptr ) { 
            return except_t( "ERROR: MongoDB Insert failed: invalid filter argument" );
        }

        bson_t* b_options= json_to_bson( options );
        
        mongoc_cursor_t *cursor = mongoc_collection_find_with_opts( collection, b_filter, b_options, NULL );
        array_t<object_t> results; const bson_t *doc;

        while( mongoc_cursor_next( cursor, &doc ) ){ results.push( bson_to_json(doc) ); }

        if( b_filter  ){ bson_destroy( b_filter  ); }
        if( b_options ){ bson_destroy( b_options ); }
        mongoc_cursor_destroy( cursor );
        
    return results; }

    /*─······································································─*/

    expected_t<object_t,except_t>
    insert( string_t db, string_t table, object_t data, object_t options={} ) const noexcept {

        auto collection = get_collection( db, table );
        bson_t  reply; bson_error_t error;

        bson_t* b_data    = json_to_bson( data ); if( b_data==nullptr ) { 
            return except_t( "ERROR: MongoDB Insert failed: invalid data argument" );
        }

        bson_t* b_options = json_to_bson( options );

        bool success = mongoc_collection_insert_one( collection, b_data, b_options, &reply, &error );
        
        if( b_data    ){ bson_destroy( b_data );    }
        if( b_options ){ bson_destroy( b_options ); }

        if( !success ) { 
            bson_destroy(&reply);
            return except_t( "ERROR: MongoDB Insert failed:", error.message ); 
        }

        object_t out = bson_to_json(&reply); 
        /*----------*/ bson_destroy(&reply);

    return out; }

    /*─······································································─*/

    expected_t<object_t,except_t>
    update( string_t db, string_t table, object_t filter, object_t update, object_t option={} ) const noexcept {

        auto grp = get_collection( db, table ); 
        bson_t reply; bson_error_t error;

        bson_t* b_filter = json_to_bson( filter ); if( b_filter==nullptr ) { 
            return except_t( "ERROR: MongoDB Insert failed: invalid filter argument" );
        }

        bson_t* b_update = json_to_bson( update ); if( b_update==nullptr ) { 
            if( b_filter ) { bson_destroy( b_filter ); }
            return except_t( "ERROR: MongoDB Insert failed: invalid update argument" );
        }

        bson_t* b_option = json_to_bson( option );

        bool success = mongoc_collection_update_one( 
             grp, b_filter, b_update, b_option, &reply, &error 
        );

        if( b_filter ){ bson_destroy( b_filter ); }
        if( b_update ){ bson_destroy( b_update ); }
        if( b_option ){ bson_destroy( b_option ); }

        if( !success ) {
            bson_destroy( &reply );
            return except_t( "ERROR: MongoDB Update failed:", error.message );
        }

        object_t out = bson_to_json( &reply );
        /*----------*/ bson_destroy( &reply ); 
        
    return out; }

    /*─······································································─*/

    expected_t<object_t,except_t>
    remove( string_t db, string_t table, object_t filter, object_t option={} ) const noexcept {

        auto grp = get_collection( db, table );
        bson_t reply; bson_error_t error;

        bson_t* b_filter = json_to_bson( filter ); if( b_filter==nullptr ) { 
            return except_t( "ERROR: MongoDB Insert failed: invalid filter argument" );
        }

        bson_t* b_option = json_to_bson( option );

        bool success = mongoc_collection_delete_one( 
             grp, b_filter, b_option, &reply, &error 
        );

        if( b_filter ){ bson_destroy( b_filter ); }
        if( b_option ){ bson_destroy( b_option ); }

        if( !success ) {
            bson_destroy( &reply );
            return except_t( "ERROR: MongoDB Delete failed:", error.message );
        }

        object_t out = bson_to_json( &reply );
        /*----------*/ bson_destroy( &reply ); 
        
    return out; }

}; }

/*────────────────────────────────────────────────────────────────────────────*/

#endif

/*────────────────────────────────────────────────────────────────────────────*/