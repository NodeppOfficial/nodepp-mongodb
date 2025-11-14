#ifndef NODEPP_MONGODB
#define NODEPP_MONGODB

/*────────────────────────────────────────────────────────────────────────────*/

#include <nodepp/nodepp.h>
#include <nodepp/json.h>

#include <mongoc/mongoc.h>
#include <bson/bson.h>

/*────────────────────────────────────────────────────────────────────────────*/

namespace { void inline start_mongodb() {

    static bool enabled = false; 
    if( enabled ){ return; }
    
    nodepp::process::onSIGEXIT.once([=](){
        mongoc_cleanup(); /*--------*/
    }); mongoc_init(); enabled = true; 

}}

/*────────────────────────────────────────────────────────────────────────────*/

namespace nodepp { class mongodb_t {
protected:

    struct NODE {

        mongoc_uri_t    *uri = nullptr;
        mongoc_client_t *ctx = nullptr;
        map_t< string_t,mongoc_collection_t*> list;

    };  ptr_t<NODE> obj;

    /*─······································································─*/

    mongoc_collection_t* get_collection( string_t db, string_t table ) const noexcept {
        string_t id = regex::format( "${0}_${1}", db, table );
        if( obj->list.has( id ) ){ return obj->list[id]; }
        obj->list[id] = mongoc_client_get_collection( obj->ctx, db.get(), table.get() );
    }

    void clear_collection() const noexcept {
        auto x=obj->list.raw().first(); while( x!=nullptr ){
        auto y=x->next; mongoc_collection_destroy( x->data );
        x=y; }
    }

    /*─······································································─*/

    bson_t* json_to_bson( object_t obj ) const {

        if( !obj.has_value() ){ return nullptr; }

        bson_error_t error; bson_t* doc = bson_new_from_json(
            (const uint8_t*) json::stringify( obj ).get(),
            -1, &error
        ); 

        if( !doc )
          { throw except_t( "ERROR: Failed to parse JSON string:", error.message ); }

        return doc;
        
    }

    object_t bson_to_json( bson_t* obj ) const {

        char *str; size_t len; object_t out;

        str = bson_as_canonical_extended_json( obj, &len );
        out = json::parse( string_t( str, len ) );

        bson_free(str); return out;

    }

public:

    mongodb_t( string_t uri ) : obj( new NODE() ) { 
        
        start_mongodb(); bson_error_t error;
        
        obj->uri = mongoc_uri_new_with_error( uri.get(), &error );

        if( !obj->uri ) 
          { throw except_t( "Failed to parse URI:", error.message ); }
        
        obj->ctx = mongoc_client_new_from_uri( obj->uri );

        if( !obj->ctx ) 
          { throw except_t( "Failed to create MongoDB client" ); }

    }

    /*─······································································─*/

    mongodb_t() : obj( new NODE() ) /*------*/ { start_mongodb(); }

   ~mongodb_t() noexcept { if( obj.count()>1 ){ return; } free(); }

    /*─······································································─*/

    array_t<object_t> find( string_t db, string_t table, object_t object, object_t option ) const {
    
        auto grp = get_collection( db, table ); bson_error_t error;
        
        bson_t* doc = json_to_bson( filter );
        bson_t* opt = json_to_bson( option );
        
        queue_t<object_t> results;
        
        mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
            grp, doc, opt, NULL
        );  const bson_t *doc;

        while( mongoc_cursor_next( cursor, &doc ) ) 
             { results.push( bson_to_json( (bson_t*)doc ) ); }

            if( doc ){ bson_destroy( doc ); }
            if( opt ){ bson_destroy( opt ); }
            mongoc_cursor_destroy (cursor);
        
        return results.data();
        
    }

    /*─······································································─*/

    object_t insert( string_t db, string_t table, object_t object, object_t option ) const {

        auto grp = get_collection( db, table ); bson_error_t error;

        bson_t  tmp ;
        bson_t* obj = json_to_bson( object );
        bson_t* opt = json_to_bson( option );

        if( !mongoc_collection_insert_one( grp, obj, opt, &tmp, &error ) ) 
          { throw except_t( "ERROR: MongoDB Insert failed:", error.message ); }

        object_t out = bson_to_json( &tmp );
        
        if( obj != nullptr ){ bson_destroy(obj); } bson_destroy(&tmp);
        if( opt != nullptr ){ bson_destroy(opt); } return out;

    }

    /*─······································································─*/

    void free() const noexcept { clear_collection();
        
        if( obj->ctx != nullptr ){ mongoc_client_destroy(obj->ctx); }
        if( obj->uri != nullptr ){ mongoc_uri_destroy(obj->uri); }
        
    }

}; }

/*────────────────────────────────────────────────────────────────────────────*/

#endif

/*────────────────────────────────────────────────────────────────────────────*/