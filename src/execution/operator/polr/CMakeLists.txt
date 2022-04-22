add_library_unity(duckdb_operator_multiplexer OBJECT physical_multiplexer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_multiplexer>
    PARENT_SCOPE)

add_library_unity(duckdb_operator_adaptive_union OBJECT
                  physical_adaptive_union.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_adaptive_union>
    PARENT_SCOPE)
