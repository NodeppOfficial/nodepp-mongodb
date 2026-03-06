# Nodepp-MongoDB

```cpp
#include <nodepp/nodepp.h>
#include <nodepp/mongodb.h>

using namespace nodepp;

void onMain() {

    // 1. Initialize Connection
    mongodb_t db( "mongodb://localhost:27017" );

    try {

        // 2. Prepare Data (using nodepp object_t/json)
        object_t new_user = {
            { "name", "Jane Doe" },
            { "email", "jane@example.com" },
            { "active", true },
            { "roles", array_t<string_t>({ "admin", "editor" }) }
        };

        // 3. Perform Insert
        auto result = db.insert( "app_db", "users", new_user );
        console::log( "User Inserted:", result );

        // 4. Perform Find (Query for active admins)
        object_t query = { { "roles", "admin" }, { "active", true } };
        auto admins = db.find( "app_db", "users", query );

        console::log( "Active Admins Found:" );
        for ( auto& admin : admins ) {
            console::log( " - ", admin["name"] );
        }

    } catch ( const except_t& e ) {
        console::error( e.what() );
    }

}
```

```cpp
// 1. Update: Change user status
object_t query  ({ 
    { "email", "jane@example.com" } 
});

object_t change = ({ 
    { "$set", object_t({ 
        { "active", false }
    }) }
});

auto up_res = db.update( "app_db", "users", query, change );
console::log( "Update Stats:", up_res ); 

// 2. Delete: Remove a specific user
auto del_res = db.remove( "app_db", "users", query );
console::log( "Deleted Count:", del_res["deletedCount"] );
```